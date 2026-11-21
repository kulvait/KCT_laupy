#!/usr/bin/env python
#Author: Vojtech Kulvait
#Description: GPU management through Redis server BRPOP and LPUSH
#Date: 2024-2026
#License: GPL-3.0

import argparse
import subprocess
import datetime
import os
import re
import sys
import threading
import time
import logging

import psutil
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
from termcolor import colored

from laupy.redis_server import RedisLifecycleError, RedisManager, probe_client

# Create a logger specific to this module
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)# Set the logging level to INFO
# Create a console handler and set its level to INFO
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s : %(message)s', datefmt='%d.%m.%Y %H:%M:%S')
ch.setFormatter(formatter)
# Add the handler to the logger
log.addHandler(ch)
log.propagate = False  # Prevent log messages from being propagated to the root logger
# Warnings of laupy.redis_server (e.g. SIGKILL after SIGTERM when starting the server) through the same handler
logging.getLogger("laupy.redis_server").addHandler(ch)
logging.getLogger("laupy.redis_server").propagate = False

# Seconds between checks of a waiting --get for GPUs held by processes that no longer exist (purgeWhileWaiting).
# Every waiting process checks, so this is kept long; a crashed holder's GPU comes back within this time.
WAIT_PURGE_INTERVAL = 180
# Seconds an active GPU may be without a holder record before a purge returns it (see purgeNonexistentProcesses).
# Normally a GPU is without record only for milliseconds, between BRPOP and writing the record in --get.
NO_HOLDER_GRACE = 60

def buildParser():
	parser = argparse.ArgumentParser(description="GPU management through Redis server")
	
	parser.add_argument("--redis-server", type=str, default="localhost", help="Redis server address")
	parser.add_argument("--redis-port", type=int, default=6379, help="Redis server port")
	parser.add_argument("--redis-db", type=int, default=0, help="Redis server db")
	parser.add_argument("--redis-prefix", type=str, default="GPU", help="Prefix of Redis objects to be used for GPU management")
	parser.add_argument("--pid", type=int, default=None, help="Process that holds the GPU for --get and --release, instead of the parent process. Pass the long-running process that uses the GPU: when it ends, its GPU is returned to the pool.")
	parser.add_argument("--force", action="store_true", help="With --redis-manage*: replace an existing pool. With --redis-delete: delete even while GPUs are in use. Replacing is refused while running processes hold GPUs.")
	parser.add_argument("--try-start-server", action="store_true", help="Ensure local Redis exists, then initialize or reuse a matching GPU pool",)
	parser.add_argument("--kill-unresponsive-server", action="store_true", help="With --try-start-server: if your own redis-server holds the port but does not respond, stop it (SIGTERM, then SIGKILL) and start a new one")
	parser.add_argument("--managed-platform-id", type=int, help="Specify the OpenCL platform ID to be managed. If not specified, the default platform will be used. Use only with --redis-manage-all, --redis-manage or --redis-manage-count.")
	action = parser.add_mutually_exclusive_group(required=True)
	#System information
	action.add_argument("--platform-list", action="store_true", help="List all OpenCL platforms and devices.")
	action.add_argument("--default-platform-id", action="store_true", help="Suggest the CUDA OpenCL platform with the most devices, and print its ID.")
	#Redis management
	action.add_argument("--redis-manage-all", action="store_true", help="Initialize GPU_IDLE, GPU_MANAGED and GPU_EVENTS objects with all available GPU.")
	action.add_argument("--redis-manage", type=str, help="Initialize GPU_IDLE, GPU_MANAGED and GPU_EVENTS objects with specified GPUs provided as comma separated list.")
	action.add_argument("--redis-manage-count", type=int, help="Initialize GPU_IDLE, GPU_MANAGED and GPU_EVENTS objects with N GPUs, where N is the number of GPUs to be managed.")
	action.add_argument("--redis-delete", action="store_true", help="Delete all Redis objects related to GPU management (refused while GPUs are in use or processes wait for one, unless --force).")
	action.add_argument("--redis-purge", action="store_true", help="Return the GPUs of holders that no longer exist (a waiting --get also does this every few minutes).")
	action.add_argument("--redis-managed-insert", default=None, type=int, help="Insert GPU ID to managed IDs.")
	action.add_argument("--redis-managed-delete", default=None, type=int, help="Remove GPU ID from managed IDs.")
	#GPU management and database information
	action.add_argument("--get", action="store_true", help="Get GPU ID that is not in use.")
	action.add_argument("--release", default=None, type=int, help="Release GPU ID after it is no longer in use")
	action.add_argument("--managed-gpu-count", action="store_true", help="Get number of GPUs managed by the Redis server")
	action.add_argument("--managed", action="store_true", help="Print managed GPUs.")
	action.add_argument("--log", action="store_true", help="Print log.")
	action.add_argument("--info", action="store_true", help="Print GPUs summary.")
	action.add_argument("--idle", action="store_true", help="Print idle GPUs.")
	action.add_argument("--active", action="store_true", help="Print active GPUs.")
	action.add_argument("--platform-id", action="store_true", help="Print Platform ID that is managed by the Redis server.")
	action.add_argument("--gpu-count", action="store_true", help="Get number of devices on managed platform.")
	return parser

class DiscoveryError(RuntimeError):
	pass

def getOpenCLPlatforms():
	env = os.environ.copy()
	env["LC_ALL"] = "C"
	try:
		result = subprocess.run(
			["clinfo", "-l"],
			capture_output=True,
			text=True,
			encoding="utf-8",
			errors="replace",
			timeout=30,
			check=True,
			env=env,
		)
	except FileNotFoundError as exc:
		raise DiscoveryError(
			"clinfo was not found; install it and ensure it is on PATH."
		) from exc
	except subprocess.TimeoutExpired as exc:
		raise DiscoveryError("clinfo -l timed out after 30 seconds.") from exc
	except subprocess.CalledProcessError as exc:
		detail = (exc.stderr or exc.stdout or "").strip()
		raise DiscoveryError(
			f"clinfo -l failed with exit code {exc.returncode}: {detail}"
		) from exc
	except OSError as exc:
		raise DiscoveryError(f"Could not execute clinfo: {exc}") from exc
	platforms = []
	current = None
	for line in result.stdout.splitlines():
		if not line.strip():
			continue
		platform_match = re.fullmatch(
			r"\s*Platform #(\d+):\s*(.+?)\s*", line
		)
		if platform_match:
			current = {
				"id": int(platform_match.group(1)),
				"name": platform_match.group(2),
				"devices": [],
			}
			platforms.append(current)
			continue
		device_match = re.fullmatch(
			r"\s*[+`|\\─├└│ -]*Device #(\d+):\s*(.+?)\s*", line
		)
		if device_match and current is not None:
			current["devices"].append({
				"id": int(device_match.group(1)),
				"name": device_match.group(2),
			})
			continue
		raise DiscoveryError(
			f"Unrecognized clinfo -l output: {line!r}"
		)
	if not platforms:
		raise DiscoveryError("clinfo -l reported no OpenCL platforms.")
	return platforms

def getDefaultPlatformID(platforms=None):
	if platforms is None:
		platforms = getOpenCLPlatforms()
	admissible_platforms = [p for p in platforms if "cuda" in p["name"].lower()]
	if not admissible_platforms:
		admissible_platforms = [p for p in platforms if len(p["devices"]) > 0]
	admissible_platforms.sort(key=lambda p: len(p["devices"]), reverse=True)
	if admissible_platforms and len(admissible_platforms[0]["devices"]) > 0:
		return admissible_platforms[0]["id"]
	else:
		raise DiscoveryError("No OpenCL platforms with devices found.")

def checkPID(PID):
	"""True if a process with this PID exists.

	Signal 0 only checks whether the process could be signalled. A process of another user exists but
	may not be signalled (PermissionError), so it counts as running: its GPU must not be released.
	"""
	try:
		os.kill(PID, 0)
	except ProcessLookupError:
		return False
	except PermissionError:
		return True
	except OSError:
		return False
	return True

def processStartTime(pid):
	"""Start time of a process (seconds since the epoch), or None if it can not be read."""
	try:
		return psutil.Process(pid).create_time()
	except (psutil.Error, OSError):
		return None

def holderAlive(pid, start=None):
	"""True while the process holding a GPU still runs.

	start is the holder's start time recorded at --get: after the holder ended, the kernel may give its PID to an
	unrelated process, which then has another start time and does not count. Without a recorded start time, or when
	it can not be read, the PID alone decides (checkPID: processes of other users count as running).
	"""
	if not checkPID(pid):
		return False
	if start is None:
		return True
	try:
		return abs(psutil.Process(pid).create_time() - float(start)) < 1.0
	except psutil.NoSuchProcess:
		return False
	except (psutil.Error, OSError, ValueError):
		return True

# Atomic pool operations.
# Several processes use the pool at the same time (gets, releases, purges, a second job setting up the pool).
# Whenever a step first checks the state ("is the GPU idle?", "does the pool exist?") and then changes it, another
# process can act in between, and a GPU ends up twice in the idle list: two later --get calls then hand out the same
# card. Redis runs a Lua script as one step that no other client can interrupt, so check and change happen together.

# Return a GPU to the idle list, unless it is already there or no longer held by the expected process.
#   KEYS[1]: idle list, KEYS[2]: holder hash; ARGV[1]: GPU ID,
#   ARGV[2]: expected holder PID ('' = any holder, '-' = only if there is no holder record at all)
#   Returns 1 if returned, 0 if it was already idle, -1 if the holder is not the expected one (taken meanwhile).
RETURN_GPU_SCRIPT = """
for _, id in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
	if id == ARGV[1] then return 0 end
end
if ARGV[2] == '-' then
	if redis.call('EXISTS', KEYS[2]) == 1 then return -1 end
elseif ARGV[2] ~= '' and redis.call('HGET', KEYS[2], 'PID') ~= ARGV[2] then return -1 end
redis.call('DEL', KEYS[2])
redis.call('LPUSH', KEYS[1], ARGV[1])
return 1
"""

def returnGPU(rs, prefix, gpu_id, expected_pid=None, require_no_holder=False):
	"""Atomically put a GPU back to the idle list and forget its holder (see RETURN_GPU_SCRIPT).

	expected_pid: return it only if this PID still holds it, so that a GPU which was meanwhile returned and taken by
	another process is not taken away from it. require_no_holder: return it only if it still has no holder record.
	Returns 1 (returned), 0 (already idle) or -1 (other holder).
	"""
	script = rs.register_script(RETURN_GPU_SCRIPT)
	condition = "-" if require_no_holder else ("" if expected_pid is None else str(expected_pid))
	return int(script(keys=["%s_IDLE"%(prefix), "%s_GPU%d"%(prefix, int(gpu_id))], args=[int(gpu_id), condition]))

# Add a GPU to the pool unless it is managed already (two simultaneous inserts must not add it twice).
#   KEYS[1]: managed list, KEYS[2]: idle list; ARGV[1]: GPU ID. Returns 1 if added, 0 if already managed.
INSERT_GPU_SCRIPT = """
for _, id in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
	if id == ARGV[1] then return 0 end
end
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[1])
return 1
"""

# Create the pool in one step unless it exists (or replace it with force), so that two jobs setting up the pool at the
# same moment can not both fill it.
#   KEYS: 1 idle list, 2 managed list, 3 events, 4 INIT hash, 5 PLATFORM hash, 6 NOHOLDER hash
#   ARGV: 1 force ('1' or '0'), 2 time, 3 managed IDs comma separated, 4 GPU count, 5 platform ID, 6 platform name,
#         7 platform device count, 8 event message, 9... managed GPU IDs
#   Returns 1 if the pool was created, 0 if it already existed.
CREATE_POOL_SCRIPT = """
if ARGV[1] ~= '1' and redis.call('EXISTS', KEYS[4]) == 1 then return 0 end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[6])
for i = 9, #ARGV do
	redis.call('LPUSH', KEYS[1], ARGV[i])
	redis.call('LPUSH', KEYS[2], ARGV[i])
end
redis.call('HSET', KEYS[4], 'TIME', ARGV[2], 'GPUMANAGED', ARGV[3], 'GPUCOUNT', ARGV[4])
redis.call('HSET', KEYS[5], 'ID', ARGV[5], 'NAME', ARGV[6], 'DEVICECOUNT', ARGV[7])
redis.call('LPUSH', KEYS[3], ARGV[8])
return 1
"""

def createPool(rs, prefix, managedList, GPUcount, managedPlatform, force=False):
	"""Atomically create the pool (see CREATE_POOL_SCRIPT); True if created, False if it already existed."""
	now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	event = "%s: Redis initialized, %d managed GPUs, platform ID %d."%(now, len(managedList), managedPlatform["id"])
	script = rs.register_script(CREATE_POOL_SCRIPT)
	keys = ["%s_%s"%(prefix, name) for name in ("IDLE", "MANAGED", "EVENTS", "INIT", "PLATFORM", "NOHOLDER")]
	args = ["1" if force else "0", now, ",".join(str(i) for i in managedList), GPUcount, managedPlatform["id"],
	        managedPlatform["name"], len(managedPlatform["devices"]), event] + [int(i) for i in managedList]
	return int(script(keys=keys, args=args)) == 1

def initRedisObjects(rs, prefix, managedList, GPUcount, managedPlatform, force=False, try_start_server=False):
	if createPool(rs, prefix, managedList, GPUcount, managedPlatform, force=force):
		return 0
	# The pool exists (possibly created by another process a moment ago)
	lastinit_data = rs.hgetall("%s_INIT"%(prefix))
	if try_start_server:
		# If the server was started by this script, but INIT data already exists, return true only if GPUcount and managedList match the existing data, otherwise fail with an error.
		existing_gpu_count = int(lastinit_data.get("GPUCOUNT", 0))
		existing_managed = lastinit_data.get("GPUMANAGED", "")
		existing_managed_list = [int(i) for i in existing_managed.split(",") if i.strip() != ""]
		if existing_gpu_count != GPUcount:
			print("Error: Existing GPU count (%d) does not match requested GPU count (%d). Use --force to reinitialize." % (existing_gpu_count, GPUcount), file=sys.stderr)
			return -1
		if sorted(existing_managed_list) != sorted(managedList):
			print("Error: Existing managed GPU list (%s) does not match requested managed GPU list (%s). Use --force to reinitialize." % (existing_managed_list, managedList), file=sys.stderr)
			return -1
		existing_platform_id = rs.hget("%s_PLATFORM"%(prefix), "ID")
		if existing_platform_id is None:
			print("Error: The existing pool has no platform record (left by an older version?). Replace it with --force.", file=sys.stderr)
			return -1
		if managedPlatform["id"] != int(existing_platform_id):
			print("Error: Existing managed platform ID (%d) does not match requested managed platform ID (%d). Use --force to reinitialize." % (int(existing_platform_id), managedPlatform["id"]), file=sys.stderr)
			return -1
		# If all checks pass, purge the existing data without reinitializing, since the server was just started and the data is consistent.
		purgeNonexistentProcesses(rs, prefix)
		return 0
	else:
		print("Objects already initialized, add force to proceed.", file=sys.stderr)
		return -1

def getManagedPlatformID(rs, prefix):
	value = rs.hget(f"{prefix}_PLATFORM", "ID")
	if value is None:
		raise DiscoveryError(
			"No managed platform is stored. Initialize the pool first, "
			"or use --default-platform-id for a suggestion."
		)
	return int(value)

def getPlatformByID(platforms, platform_id):
	for platform in platforms:
		if platform["id"] == platform_id:
			return platform
	raise DiscoveryError(f"No OpenCL platform with ID {platform_id} found.")

def getPlatformDeviceIDs(platform):
	device_ids = [d["id"] for d in platform["devices"]]
	device_ids = sorted(set(device_ids))  # Ensure unique and sorted device IDs
	return device_ids

def getManagedPlatformDeviceIDs(rs, prefix):
	managed_platform_id = getManagedPlatformID(rs, prefix)
	managed_platform = getPlatformByID(getOpenCLPlatforms(), managed_platform_id)
	device_ids = getPlatformDeviceIDs(managed_platform)
	if not device_ids:
		raise DiscoveryError(f"Managed platform {managed_platform_id} has no devices.")
	expected = list(range(device_ids[0], device_ids[0] + len(device_ids)))
	if device_ids != expected:
		print(
			"Warning: Non-contiguous device IDs on managed platform %d: %s"
			% (managed_platform_id, ",".join(str(i) for i in device_ids)),
			file=sys.stderr,
		)
	return device_ids
 
def getManagedGPUs(rs, prefix):
	managed = rs.lrange("%s_MANAGED"%(prefix), 0, -1)
	managedlist = [int(i) for i in managed]
	managedlist.sort()
	return managedlist

def getIDLEGPUs(rs, prefix):
	idle = rs.lrange("%s_IDLE"%(prefix), 0, -1)
	idlelist = [int(i) for i in idle]
	idlelist.sort()
	return idlelist

def getActiveGPUs(rs, prefix):
	managedlist = getManagedGPUs(rs, prefix)
	idlelist = getIDLEGPUs(rs, prefix)
	active = [i for i in managedlist if i not in idlelist]
	active.sort()
	return active

def getGPUCount(rs, prefix):
	platform_data = rs.hgetall(f"{prefix}_PLATFORM")
	if not platform_data:
		raise DiscoveryError(
			"No managed platform is stored. Initialize the pool first, "
			"or use --default-platform-id for a suggestion."
		)
	return int(platform_data.get("DEVICECOUNT", 0))

def insertGPU(rs, prefix, GPUID):
	valid_ids = set(getManagedPlatformDeviceIDs(rs, prefix))
	if GPUID not in valid_ids:
		print("GPU ID %d is not present on managed platform. Valid IDs: %s"  % (GPUID, ",".join(str(i) for i in sorted(valid_ids))), file=sys.stderr)
		return 1
	script = rs.register_script(INSERT_GPU_SCRIPT)
	if not int(script(keys=["%s_MANAGED"%(prefix), "%s_IDLE"%(prefix)], args=[int(GPUID)])):
		print("GPU %d is already managed."%(GPUID), file=sys.stderr)
		return 1
	msg = "%s: GPU %d is now managed."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), GPUID)
	rs.lpush("%s_EVENTS"%(prefix), msg)
	print(msg)
	return 0

def removeGPU(rs, prefix, GPUID):
	managed = getManagedGPUs(rs, prefix)
	if GPUID not in managed:
		print("GPU %d is not managed."%(GPUID), file=sys.stderr)
		return 1
	else:
		msg = "%s: GPU %d is no longer managed."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), GPUID)
		rs.lpush("%s_EVENTS"%(prefix), msg)
		rs.lrem("%s_MANAGED"%(prefix), 0, GPUID)
		rs.lrem("%s_IDLE"%(prefix), 0, GPUID)
		rs.hdel("%s_NOHOLDER"%(prefix), GPUID)
		print(msg)
		return 0

def purgeNonexistentProcesses(rs, prefix):
	"""Return the GPUs whose holder ended without --release, and GPUs that stay without holder record.

	A GPU is without holder record between BRPOP and writing the record in --get, normally for milliseconds. If
	--get is killed exactly then, the GPU is neither idle nor held and would be lost for good. Such a GPU is marked
	when first seen (hash <prefix>_NOHOLDER) and returned by a later purge once NO_HOLDER_GRACE seconds have passed.
	"""
	activeList = getActiveGPUs(rs, prefix)
	marks_key = "%s_NOHOLDER"%(prefix)
	marks = rs.hgetall(marks_key)
	without_holder = set()
	for i in activeList:
		record = rs.hgetall("%s_GPU%d"%(prefix, int(i)))
		if record.get("PID") is not None:
			PID = int(record["PID"])
			# Returned only while the dead PID still holds it: another purge may have returned it meanwhile, and a
			# --get may already have handed it to a new holder
			if not holderAlive(PID, record.get("START")) and returnGPU(rs, prefix, i, expected_pid=PID) == 1:
				LOG = "%s: GPU %s acquired by process with PID %d, was released by redis purge as the process with given PID does no longer exist."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), i, PID)
				rs.lpush("%s_EVENTS"%(prefix), LOG)
			continue
		without_holder.add(str(i))
		first_seen = marks.get(str(i))
		if first_seen is None:
			rs.hsetnx(marks_key, i, time.time())	# First time seen without holder record
		elif time.time() - float(first_seen) > NO_HOLDER_GRACE:
			# Returned only if it still has no holder record (the --get may just have written one)
			if returnGPU(rs, prefix, i, require_no_holder=True) == 1:
				LOG = "%s: GPU %s was in use without holder record for more than %d s (an interrupted --get?), returned by redis purge."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), i, NO_HOLDER_GRACE)
				rs.lpush("%s_EVENTS"%(prefix), LOG)
			rs.hdel(marks_key, i)
	# Marks of GPUs that got a holder record or became idle meanwhile are no longer needed
	stale = [gpu for gpu in marks if gpu not in without_holder]
	if stale:
		rs.hdel(marks_key, *stale)

def liveHolders(rs, prefix):
	"""[(GPU ID, PID, TIME)] of active GPUs whose recorded holder process is still running."""
	holders = []
	for i in getActiveGPUs(rs, prefix):
		record = rs.hgetall("%s_GPU%d"%(prefix, int(i)))
		if record.get("PID") is not None and holderAlive(int(record["PID"]), record.get("START")):
			holders.append((i, int(record["PID"]), record.get("TIME", "?")))
	return holders

def waitingClients(rs):
	"""Number of clients of this Redis server blocked in a waiting command, e.g. a GPU --get in BRPOP.

	Redis does not tell which list a client waits for, so these may also wait for another pool on the same server.
	"""
	return sum(1 for client in rs.client_list() if "b" in client.get("flags", ""))

def purgeWhileWaiting(make_client, prefix, stop):
	"""Watcher thread of a waiting --get: every WAIT_PURGE_INTERVAL seconds, return the GPUs of holders that no
	longer exist, until stop is set.

	The waiting main thread stays in BRPOP and keeps its place in the queue; a GPU returned here goes to the process
	that has waited longest. It uses its own Redis client (make_client), as a client must not be shared between
	threads. Several waiters purging at the same time is harmless, returning a GPU is atomic (returnGPU).
	"""
	rs = make_client()
	while not stop.wait(WAIT_PURGE_INTERVAL):
		try:
			purgeNonexistentProcesses(rs, prefix)
		except redis.RedisError as e:
			log.warning(f"Checking for GPUs of terminated processes failed: {e}")

def main_with_discovery(argv=None):
	# Parsing and validation
	parser = buildParser()
	ARG = parser.parse_args(argv)
	if ARG.managed_platform_id is not None:
		if not (ARG.redis_manage_all or ARG.redis_manage is not None or ARG.redis_manage_count is not None):
			parser.error("--managed-platform-id can only be used with --redis-manage-all, --redis-manage or --redis-manage-count.")
	if ARG.redis_manage_count is not None and ARG.redis_manage_count <= 0:
		parser.error("--redis-manage-count must be a positive integer.")
	if ARG.kill_unresponsive_server and not ARG.try_start_server:
		parser.error("--kill-unresponsive-server requires --try-start-server")
	if ARG.try_start_server:
		if not (ARG.redis_manage_all or ARG.redis_manage is not None or ARG.redis_manage_count is not None):
			parser.error("--try-start-server requires a --redis-manage* action")
		if ARG.redis_server not in ("localhost", "127.0.0.1"):
			parser.error("--try-start-server requires a local loopback endpoint")
		if ARG.force:
			parser.error("--force cannot be combined with --try-start-server")
		if not 1 <= ARG.redis_port <= 65535:
			parser.error("--redis-port must be between 1 and 65535")
	
	if ARG.platform_list:
		platforms = getOpenCLPlatforms()
		default_platform_id = None
		try:
			default_platform_id = getDefaultPlatformID(platforms)
		except DiscoveryError:
			pass
		for platform in platforms:
			if platform["id"] == default_platform_id:
				print(f"Platform ID: {platform['id']}, Name: {platform['name']} - Default Platform")
			else:
				print(f"Platform ID: {platform['id']}, Name: {platform['name']}")
			for device in platform["devices"]:
				print(f"  Device ID: {device['id']}, Name: {device['name']}")
		return 0
	
	if ARG.default_platform_id:
		try:
			default_platform_id = getDefaultPlatformID()
			print(f"{default_platform_id}")
			return 0
		except DiscoveryError as e:
			print(f"Error: {e}", file=sys.stderr)
			return 1
	
	rs = redis.Redis(host=ARG.redis_server, port=ARG.redis_port, db=ARG.redis_db, socket_connect_timeout=10, socket_timeout=None, decode_responses=True) #GPU acquisition might wait for a long time, so we set socket_timeout to None to avoid timeout errors
	try:
		# Quick check, if it fails we may try to start the server. A hung server accepts the connection but
		# never answers, so the probe client gives up after a few seconds (see laupy.redis_server.probe_client).
		db = probe_client(ARG.redis_server, ARG.redis_port, db=ARG.redis_db, decode_responses=True)
		db.ping()
	except (redis.ConnectionError, redis.TimeoutError) as e:
		# Unreachable or hung: with --try-start-server, starting the server sorts it out (or refuses, see its message)
		log.warning(f"Can not connect to Redis server: {e}")
		if ARG.try_start_server:
			try:
				log.info("Trying to start Redis server...")
				# Same as REDIS start --port <port> (laupy.redis_server, no dependency on REDIS being on PATH); logs
				# by default (timestamped in ./log). Its messages are a side note here: GPU's stdout carries only
				# results that scripts capture (e.g. N=$(GPU --redis-manage-all --try-start-server) must get just
				# the GPU count), so they go to stderr, where the user still sees them.
				manager = RedisManager(port=ARG.redis_port, out=lambda message: print(message, file=sys.stderr))
				manager.start(kill_unresponsive=ARG.kill_unresponsive_server)
				db.ping()	# Quick-check client: a server hanging right after start can not block us
				log.info("Redis server started successfully.")
			except (RedisLifecycleError, OSError, subprocess.SubprocessError) as e:
				log.error(f"Failed to start Redis server: {e}")
				return 1
			except (redis.ConnectionError, redis.TimeoutError):
				log.error("Redis server did not respond after starting.")
				return 1
		else:
			print("Can not connect to Redis server, try to start server first.", file=sys.stderr)
			return 1
	
	# Redis management setup --redis-manage-all, --redis-manage or --redis-manage-count
	if ARG.redis_manage_all or ARG.redis_manage is not None or ARG.redis_manage_count is not None:
		platforms = getOpenCLPlatforms()
		managed_platform_id = ARG.managed_platform_id if ARG.managed_platform_id is not None else getDefaultPlatformID(platforms)
		managed_platform = getPlatformByID(platforms, managed_platform_id)
		platform_device_ids = getPlatformDeviceIDs(managed_platform)
		GPUcount = len(platform_device_ids)
		managedList = []
		if ARG.redis_manage_all:
			managedList = platform_device_ids[:]
		elif ARG.redis_manage is not None:
			try:
				raw = [x.strip() for x in ARG.redis_manage.split(",") if x.strip() != ""]
				managedList = [int(i) for i in raw]
			except ValueError:
				print("Invalid --redis-manage list. Use comma-separated integers, e.g. 0,1,3", file=sys.stderr)
				return 1
			managedList = sorted(set(managedList))
			if len(managedList) == 0:
				print("No valid GPU IDs provided in --redis-manage list.", file=sys.stderr)
				return 1
			valid_ids = set(platform_device_ids)
			out_of_range = [i for i in managedList if i not in valid_ids]
			if out_of_range:
				print(f"GPU IDs {out_of_range} are not present on managed platform {managed_platform_id}. Valid IDs: {platform_device_ids}", file=sys.stderr)
				return 1
		elif ARG.redis_manage_count is not None:
			if ARG.redis_manage_count <= 0 or ARG.redis_manage_count > GPUcount:
				print("Can not manage more GPUs than available or less than 1 but you requested %d out of %d possible!"%(ARG.redis_manage_count, GPUcount), file=sys.stderr)
				return 1
			managedList = list(reversed(platform_device_ids))[:ARG.redis_manage_count]
		if ARG.force and rs.exists("%s_INIT"%(ARG.redis_prefix)):
			# Replacing the pool puts every GPU back to the idle list. A GPU still in use would then be handed out a
			# second time, so --force is refused while running processes hold GPUs.
			holders = liveHolders(rs, ARG.redis_prefix)
			if holders:
				print("Refusing to replace the GPU pool with --force while GPUs are in use:", file=sys.stderr)
				for gpu, pid, since in holders:
					print("  GPU %d held by running PID %d since %s"%(gpu, pid, since), file=sys.stderr)
				print("Release them first (or wait until these processes end).", file=sys.stderr)
				return 1
		try:
			status = initRedisObjects(rs, ARG.redis_prefix, managedList, GPUcount, managed_platform, force=ARG.force, try_start_server=ARG.try_start_server)
			if status == -1: # Objects already initialized, add force to proceed, exit successfully.
				return 0
			else:
				print(len(managedList))
				return 0
		except Exception as e:
			print(f"Error: {e}", file=sys.stderr)
			return 1
	
	if not rs.exists("%s_PLATFORM"%(ARG.redis_prefix)):
		# No pool yet: the requested action is not possible. Explain on stderr and fail, so that a script capturing
		# the output (e.g. GPUCOUNT=$(GPU --gpu-count)) gets no text and sees the error.
		print("Not managing any platform, set Redis objects first by running --redis-manage-all, --redis-manage or --redis-manage-count, you can also use --managed-platform-id to specify the platform ID to be managed or use --default-platform-id to get a suggestion for the platform ID to be managed.", file=sys.stderr)
		try:
			platforms = getOpenCLPlatforms()
			if all(p["devices"] == [] for p in platforms):
				print("Redis not initialized, no OpenCL platforms with devices found.", file=sys.stderr)
				return 1
			default_platform_id = getDefaultPlatformID(platforms)
			default_platform = getPlatformByID(platforms, default_platform_id)
			if default_platform is not None:
				GPUcount = len(default_platform["devices"])
				print("Redis not initialized, default platform ID %d has %d GPUs."%(default_platform_id, GPUcount), file=sys.stderr)
			else:
				print("Redis not initialized, default platform ID %d not found."%(default_platform_id), file=sys.stderr)
			return 1
		except DiscoveryError:
			print("Redis not initialized, discovery of OpenCL platforms failed, no OpenCL platforms found.", file=sys.stderr)
			return 1
	
	if ARG.redis_managed_insert is not None:
		return insertGPU(rs, ARG.redis_prefix, ARG.redis_managed_insert)
	
	if ARG.redis_managed_delete is not None:
		return removeGPU(rs, ARG.redis_prefix, ARG.redis_managed_delete)

	#GPU count on managed platform
	if ARG.platform_id:
		# Read the managed platform ID from Redis
		try:
			platform_id = getManagedPlatformID(rs, ARG.redis_prefix)
			print(platform_id)
			return 0
		except DiscoveryError as e:
			print(f"Error: {e}", file=sys.stderr)
			return 1

	if ARG.gpu_count:
		# Read the managed platform ID from Redis
		try:
			gpu_count = getGPUCount(rs, ARG.redis_prefix)
			print(gpu_count)
			return 0
		except DiscoveryError as e:
			print(f"Error: {e}", file=sys.stderr)
			return 1

	if ARG.managed_gpu_count:
		devCount = len(rs.lrange("%s_MANAGED"%(ARG.redis_prefix), 0, -1))
		print(devCount)
		return 0

	if ARG.get:
		if ARG.pid is not None:
			PID = ARG.pid
		else:
			PID = os.getppid()
		# Wait for an idle GPU without a timeout: Redis serves the clients blocked in BRPOP in arrival order, so the
		# process that has waited longest gets the next free GPU. Meanwhile a watcher thread returns the GPUs of holders
		# that ended without --release (see purgeWhileWaiting), so such a GPU does not stay lost forever.
		stop_watcher = threading.Event()
		watcher = threading.Thread(target=purgeWhileWaiting, daemon=True, args=(
			lambda: redis.Redis(host=ARG.redis_server, port=ARG.redis_port, db=ARG.redis_db, socket_connect_timeout=1,
			                    socket_timeout=10, retry=Retry(NoBackoff(), 0), decode_responses=True),
			ARG.redis_prefix, stop_watcher))
		watcher.start()
		try:
			while True:
				try:
					GPUID = rs.brpop("%s_IDLE"%(ARG.redis_prefix), 0)
				except (redis.ConnectionError, redis.TimeoutError) as e:
					# E.g. the job that started the server ended, or REDIS stop: no GPU will come any more
					print("Redis server went away while waiting for a GPU: %s"%(e), file=sys.stderr)
					return 1
				ID = int(GPUID[1])
				managedList = getManagedGPUs(rs, ARG.redis_prefix)
				if ID in managedList:
					LOG = "%s: GPU %s acquired by PID %d."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ID, PID)
					rs.lpush("%s_EVENTS"%(ARG.redis_prefix), LOG)
					# New holder record in one transaction: no fields of an earlier holder remain (an old START would
					# make the new holder look dead), and PID, TIME and START appear together
					record = {"PID": PID, "TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
					start = processStartTime(PID)
					if start is not None:
						record["START"] = start
					transaction = rs.pipeline(transaction=True)
					transaction.delete("%s_GPU%d"%(ARG.redis_prefix, int(ID)))
					transaction.hset("%s_GPU%d"%(ARG.redis_prefix, int(ID)), mapping=record)
					transaction.execute()
					print(ID)
					break
		finally:
			stop_watcher.set()
		return 0
	
	if ARG.release is not None:
		ID = int(ARG.release)
		if ARG.pid is not None:
			PID = ARG.pid
		else:
			PID = os.getppid()
		if ID not in getManagedGPUs(rs, ARG.redis_prefix):
			# Removed from the pool (--redis-managed-delete) while it was held: forget the holder, do not return it
			if rs.delete("%s_GPU%d"%(ARG.redis_prefix, ID)):
				LOG = "%s: GPU %d released by PID %d; it is no longer managed, so not returned to the pool."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ID, PID)
				rs.lpush("%s_EVENTS"%(ARG.redis_prefix), LOG)
				print("GPU %d released (no longer managed, not returned to the pool)."%(ID))
				return 0
			print("GPU %d is not managed."%(ID), file=sys.stderr)
			return 1
		# Check "not idle" and return it in one atomic step; only for the holder seen now, so that a GPU released and
		# taken by another process in the meantime (e.g. a duplicate --release) is not taken from that process
		holder_pid = rs.hget("%s_GPU%d"%(ARG.redis_prefix, ID), "PID")
		result = returnGPU(rs, ARG.redis_prefix, ID, expected_pid=holder_pid)
		if result == 0:
			print("GPU %d is already idle."%(ID), file=sys.stderr)
			return 1
		if result == -1:
			print("GPU %d was released and taken by another process meanwhile; not released."%(ID), file=sys.stderr)
			return 1
		TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		if holder_pid is not None and holder_pid != str(PID):
			# Allowed (cooperative pool), but worth seeing: a wrong ID or --pid in a script frees a GPU still in use
			LOG = "%s: GPU %d held by PID %s released by another PID %d."%(TIME, ID, holder_pid, PID)
			print("Warning: GPU %d was held by PID %s, released by PID %d."%(ID, holder_pid, PID), file=sys.stderr)
		else:
			LOG = "%s: GPU %d released by PID %d."%(TIME, ID, PID)
		rs.lpush("%s_EVENTS"%(ARG.redis_prefix), LOG)
		print("GPU %d released."%(ARG.release))
		return 0
	
	if ARG.log:
		events = rs.lrange("%s_EVENTS"%(ARG.redis_prefix), 0, -1)
		for event in reversed(events):
			print(event)
		return 0
	
	if ARG.idle:
		idleList = getIDLEGPUs(rs, ARG.redis_prefix)
		print("There is %d IDLE GPUs, IDs: %s"%(len(idleList), ",".join([str(x) for x in idleList])))
		return 0
	
	if ARG.managed:
		managedlist = getManagedGPUs(rs, ARG.redis_prefix)
		print("There is %d MANAGED GPUs, IDs: %s"%(len(managedlist), ",".join([str(x) for x in managedlist])))
		return 0
	
	if ARG.info:
		GPUcount = getGPUCount(rs, ARG.redis_prefix)
		managed = rs.lrange("%s_MANAGED"%(ARG.redis_prefix), 0, -1)
		idle = rs.lrange("%s_IDLE"%(ARG.redis_prefix), 0, -1)
		print("Managed platform ID: %d, name: %s, device count: %d"%(getManagedPlatformID(rs, ARG.redis_prefix), rs.hget("%s_PLATFORM"%(ARG.redis_prefix), "NAME"), GPUcount))
		if len(managed) == 0:
			print("GPU total:%d no managed"%(GPUcount))
		else:
			print("GPU total:%d managed:%d idle:%d"%(GPUcount, len(managed), len(idle)))
		return 0
	
	if ARG.redis_delete:
		holders = liveHolders(rs, ARG.redis_prefix)
		waiting = waitingClients(rs)
		if (holders or waiting) and not ARG.force:
			# The holders' --release would fail, and processes waiting for a GPU would wait for a pool that is gone
			print("Refusing to delete the GPU pool while it is in use (use --force to delete anyway):", file=sys.stderr)
			for gpu, pid, since in holders:
				print("  GPU %d held by running PID %d since %s"%(gpu, pid, since), file=sys.stderr)
			if waiting:
				print("  %d process(es) waiting for a GPU"%(waiting), file=sys.stderr)
			return 1
		managed = rs.lrange("%s_MANAGED"%(ARG.redis_prefix), 0, -1)
		if len(managed) > 0:
			print("Redis objects related to %d GPUs management will be deleted."%(len(managed)))
		managedList = [i for i in managed]
		for i in managedList:
			rs.delete("%s_GPU%d"%(ARG.redis_prefix, int(i)))
		rs.delete("%s_IDLE"%(ARG.redis_prefix))
		rs.delete("%s_MANAGED"%(ARG.redis_prefix))
		rs.delete("%s_EVENTS"%(ARG.redis_prefix))
		rs.delete("%s_INIT"%(ARG.redis_prefix))
		rs.delete("%s_PLATFORM"%(ARG.redis_prefix))
		rs.delete("%s_NOHOLDER"%(ARG.redis_prefix))
		print("All Redis objects related to GPU management were deleted.")
		return 0
	
	if ARG.redis_purge:
		purgeNonexistentProcesses(rs, ARG.redis_prefix)
		return 0
	
	if ARG.active:
		active = getActiveGPUs(rs, ARG.redis_prefix)
		if len(active) == 0:
			print("There are no active GPUs.")
		else:
			print("There is %d ACTIVE GPUs, IDs: %s"%(len(active), ", ".join([str(i) for i in active])))
		for i in active:
			record = rs.hgetall("%s_GPU%d"%(ARG.redis_prefix, int(i)))
			PID, TIME = record.get("PID"), record.get("TIME")
			if TIME is not None and PID is not None:
				PID = int(PID)
				if holderAlive(PID, record.get("START")):
					print(colored("GPU %d acquired by running PID %6d at %s"%(i, PID, TIME), "green"))
				else:
					print(colored("GPU %d acquired by not running PID %6d at %s"%(i, PID, TIME), "red"))
			else:
				print("GPU %d acquired by unknown process."%(i))
		return 0
	return 0

def main(argv=None):
	try:
		return main_with_discovery(argv)
	except DiscoveryError as e:
		print(f"Error: {e}", file=sys.stderr)
		return 1

if __name__ == "__main__":
	sys.exit(main())
