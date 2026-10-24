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
import logging

import redis
from termcolor import colored

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

def buildParser():
	parser = argparse.ArgumentParser(description="GPU management through Redis server")
	
	parser.add_argument("--redis-server", type=str, default="localhost", help="Redis server address")
	parser.add_argument("--redis-port", type=int, default=6379, help="Redis server port")
	parser.add_argument("--redis-db", type=int, default=0, help="Redis server db")
	parser.add_argument("--redis-prefix", type=str, default="GPU", help="Prefix of Redis objects to be used for GPU management")
	parser.add_argument("--pid", type=int, default=None, help="Use this PID to identify PID for --get and --release instead of parent PID.")
	parser.add_argument("--force", action="store_true", help="Initialize Redis objects even if they are already initialized.")
	parser.add_argument("--try-start-server", action="store_true", help="Ensure local Redis exists, then initialize or reuse a matching GPU pool",)
	parser.add_argument("--kill-unresponsive-server", action="store_true", help="With --try-start-server, permit verified owned Redis listener SIGKILL")
	parser.add_argument("--managed-platform-id", type=int, help="Specify the OpenCL platform ID to be managed. If not specified, the default platform will be used. Use only with --redis-manage-all, --redis-manage or --redis-manage-count.")
	action = parser.add_mutually_exclusive_group(required=True)
	#System information
	action.add_argument("--platform-list", action="store_true", help="List all OpenCL platforms and devices.")
	action.add_argument("--default-platform-id", action="store_true", help="Suggest the CUDA OpenCL platform with the most devices, and print its ID.")
	#Redis management
	action.add_argument("--redis-manage-all", action="store_true", help="Initialize GPU_IDLE, GPU_MANAGED and GPU_EVENTS objects with all available GPU.")
	action.add_argument("--redis-manage", type=str, help="Initialize GPU_IDLE, GPU_MANAGED and GPU_EVENTS objects with specified GPUs provided as comma separated list.")
	action.add_argument("--redis-manage-count", type=int, help="Initialize GPU_IDLE, GPU_MANAGED and GPU_EVENTS objects with N GPUs, where N is the number of GPUs to be managed.")
	action.add_argument("--redis-delete", action="store_true", help="Delete all Redis objects related to GPU management.")
	action.add_argument("--redis-purge", action="store_true", help="Remove allocations by non existent processes.")
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
	try:
		os.kill(PID, 0)
	except OSError:
		return False
	else:
		return True

def initRedisObjects(rs, prefix, managedList, GPUcount, managedPlatform, force=False, try_start_server=False):
	lastinit_data = rs.hgetall("%s_INIT"%(prefix))
	if len(lastinit_data.keys()) == 0 or force:
		rs.delete("%s_IDLE"%(prefix))
		rs.delete("%s_MANAGED"%(prefix))
		rs.delete("%s_EVENTS"%(prefix))
		rs.delete("%s_INIT"%(prefix))
		for i in managedList:
			rs.lpush("%s_IDLE"%(prefix), i)
			rs.lpush("%s_MANAGED"%(prefix), i)
		rs.hset("%s_INIT"%(prefix), "TIME", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
		rs.hset("%s_INIT"%(prefix), "GPUMANAGED", ",".join([str(i) for i in managedList]))
		rs.hset("%s_INIT"%(prefix), "GPUCOUNT", GPUcount)
		storePlatform(rs, prefix, managedPlatform)
		rs.lpush("%s_EVENTS"%(prefix), "%s: Redis initialized, %d managed GPUs, platform ID %d."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(managedList), managedPlatform["id"]))
		return 0
	elif try_start_server:
		# If the server was started by this script, but INIT data already exists, return true only if GPUcount and managedList match the existing data, otherwise fail with an error.
		existing_gpu_count = int(lastinit_data.get("GPUCOUNT", 0))
		existing_managed = lastinit_data.get("GPUMANAGED", "")
		existing_managed_list = [int(i) for i in existing_managed.split(",") if i.strip() != ""]
		if existing_gpu_count != GPUcount:
			print("Error: Existing GPU count (%d) does not match requested GPU count (%d). Use --force to reinitialize." % (existing_gpu_count, GPUcount))
			return -1
		if sorted(existing_managed_list) != sorted(managedList):
			print("Error: Existing managed GPU list (%s) does not match requested managed GPU list (%s). Use --force to reinitialize." % (existing_managed_list, managedList))
			return -1
		if managedPlatform["id"] != int(rs.hget("%s_PLATFORM"%(prefix), "ID")):
			print("Error: Existing managed platform ID (%d) does not match requested managed platform ID (%d). Use --force to reinitialize." % (int(rs.hget("%s_PLATFORM"%(prefix), "ID")), managedPlatform["id"]))
			return -1
		# If all checks pass, purge the existing data without reinitializing, since the server was just started and the data is consistent.
		purgeNonexistentProcesses(rs, prefix)
		return 0
	else:
		print("Objects already initialized, add force to proceed.")
		return -1

def storePlatform(rs, prefix, platform):
	rs.hset(
		f"{prefix}_PLATFORM",
		mapping={
			"ID": platform["id"],
			"NAME": platform["name"],
			"DEVICECOUNT": len(platform["devices"]),
		},
	)

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
			% (managed_platform_id, ",".join(str(i) for i in device_ids))
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
	managed = getManagedGPUs(rs, prefix)
	if GPUID in managed:
		print("GPU %d is already managed."%(GPUID))
		return 1
	else:
		msg = "%s: GPU %d is now managed."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), GPUID)
		rs.lpush("%s_EVENTS"%(prefix), msg)
		rs.lpush("%s_MANAGED"%(prefix), GPUID)
		rs.lpush("%s_IDLE"%(prefix), GPUID)
		print(msg)
		return 0

def removeGPU(rs, prefix, GPUID):
	managed = getManagedGPUs(rs, prefix)
	if GPUID not in managed:
		print("GPU %d is not managed."%(GPUID))
		return 1
	else:
		msg = "%s: GPU %d is no longer managed."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), GPUID)
		rs.lpush("%s_EVENTS"%(prefix), msg)
		rs.lrem("%s_MANAGED"%(prefix), 0, GPUID)
		rs.lrem("%s_IDLE"%(prefix), 0, GPUID)
		print(msg)
		return 0

def purgeNonexistentProcesses(rs, prefix):
	activeList = getActiveGPUs(rs, prefix)
	for i in activeList:
		PID = rs.hget("%s_GPU%d"%(prefix, int(i)), "PID")
		if PID is not None:
			PID = int(PID)
			if not checkPID(PID):
				rs.delete("%s_GPU%d"%(prefix, int(i)))
				LOG = "%s: GPU %s acquired by process with PID %d, was released by redis purge as the process with given PID does no longer exist."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), i, PID)
				rs.lpush("%s_EVENTS"%(prefix), LOG)
				rs.lpush("%s_IDLE"%(prefix), i)

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
			print(f"Error: {e}")
			return -1
	
	rs = redis.Redis(host=ARG.redis_server, port=ARG.redis_port, db=ARG.redis_db, socket_connect_timeout=10, socket_timeout=None, decode_responses=True) #GPU acquisition might wait for a long time, so we set socket_timeout to None to avoid timeout errors
	STARTED_SERVER = False
	try:
		#Reduce timeout to 1 second for ping, if it fails, we will try to start the server
		db = redis.Redis(host=ARG.redis_server, port=ARG.redis_port, db=ARG.redis_db, socket_connect_timeout=1, socket_timeout=1, decode_responses=True)
		db.ping()
	except redis.ConnectionError:
		log.warning("Can not connect to Redis server.")
		if ARG.try_start_server:
			try:
				log.info("Trying to start Redis server...")
				CMD = ["REDIS", "start", "--log"]
				if ARG.kill_unresponsive_server:
					CMD.append("--kill-unresponsive-server")
				subprocess.run(CMD, check=True)
				rs.ping()
				STARTED_SERVER = True
				log.info("Redis server started successfully.")
			except subprocess.CalledProcessError as e:
				log.error(f"Failed to start Redis server: {e}")
				return -1
			except redis.ConnectionError:
				log.error("Redis server did not respond after starting.")
				return -1
		else:
			print("Can not connect to Redis server, try to start server first.")
			return -1
	
	# Redis management setup --redis-manage-all, --redis-manage or --redis-manage-count
	if ARG.redis_manage_all or ARG.redis_manage is not None or ARG.redis_manage_count is not None:
		platforms = getOpenCLPlatforms()
		managed_platform_id = ARG.managed_platform_id if ARG.managed_platform_id is not None else getDefaultPlatformID()
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
				print("Invalid --redis-manage list. Use comma-separated integers, e.g. 0,1,3")
				return -1
			managedList = sorted(set(managedList))
			if len(managedList) == 0:
				print("No valid GPU IDs provided in --redis-manage list.")
				return -1
			valid_ids = set(platform_device_ids)
			out_of_range = [i for i in managedList if i not in valid_ids]
			if out_of_range:
				print(f"GPU IDs {out_of_range} are not present on managed platform {managed_platform_id}. Valid IDs: {platform_device_ids}", file=sys.stderr)
				return -1
		elif ARG.redis_manage_count is not None:
			if ARG.redis_manage_count <= 0 or ARG.redis_manage_count > GPUcount:
				print("Can not manage more GPUs than available or less than 1 but you requested %d out of %d possible!"%(ARG.redis_manage_count, GPUcount))
				return -1
			managedList = list(reversed(platform_device_ids))[:ARG.redis_manage_count]
		try:
			status = initRedisObjects(rs, ARG.redis_prefix, managedList, GPUcount, managed_platform, force=ARG.force, try_start_server=ARG.try_start_server)
			if status == -1: # Objects already initialized, add force to proceed, exit successfully.
				return 0
			else:
				print(len(managedList))
				return 0
		except Exception as e:
			print(f"Error: {e}")
			return -1
	
	if not rs.exists("%s_PLATFORM"%(ARG.redis_prefix)) and ARG.platform_list is False and ARG.default_platform_id is False:
		print("Not managing any platform, set Redis objects first by running --redis-manage-all, --redis-manage or --redis-manage-count, you can also use --managed-platform-id to specify the platform ID to be managed or use --default-platform-id to get a suggestion for the platform ID to be managed.")
		try:
			platforms = getOpenCLPlatforms()
			if all(p["devices"] == [] for p in platforms):
				print("Redis not initialized, no OpenCL platforms with devices found.")
				return 0
			default_platform_id = getDefaultPlatformID()
			default_platform = getPlatformByID(platforms, default_platform_id)
			if default_platform is not None:
				GPUcount = len(default_platform["devices"])
				print("Redis not initialized, default platform ID %d has %d GPUs."%(default_platform_id, GPUcount))
			else:
				print("Redis not initialized, default platform ID %d not found."%(default_platform_id))
			return 0
		except DiscoveryError:
				print("Redis not initialized, discovery of OpenCL platforms failed, no OpenCL platforms found.")
				return -1
		return -1
	
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
			print(f"Error: {e}")
			return -1

	if ARG.gpu_count:
		# Read the managed platform ID from Redis
		try:
			gpu_count = getGPUCount(rs, ARG.redis_prefix)
			print(gpu_count)
			return 0
		except DiscoveryError as e:
			print(f"Error: {e}")
			return -1

	if ARG.managed_gpu_count:
		devCount = len(rs.lrange("%s_MANAGED"%(ARG.redis_prefix), 0, -1))
		print(devCount)
		return 0

	if ARG.get:
		if ARG.pid is not None:
			PID = ARG.pid
		else:
			PID = os.getppid()
		GPUID = rs.brpop("%s_IDLE"%(ARG.redis_prefix), 0)
		while True:
			if GPUID is not None:
				ID = int(GPUID[1])
				managedList = getManagedGPUs(rs, ARG.redis_prefix)
				if ID in managedList:
					LOG = "%s: GPU %s acquired by PID %d."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ID, PID)
					rs.lpush("%s_EVENTS"%(ARG.redis_prefix), LOG)
					rs.hset("%s_GPU%d"%(ARG.redis_prefix, int(ID)), "PID", PID)
					rs.hset("%s_GPU%d"%(ARG.redis_prefix, int(ID)), "TIME", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
					print(ID)
					break
			GPUID = rs.brpop("%s_IDLE"%(ARG.redis_prefix), 0)
		return 0
	
	if ARG.release is not None:
		ID = int(ARG.release)
		if ARG.pid is not None:
			PID = ARG.pid
		else:
			PID = os.getppid()
		idlelist = getIDLEGPUs(rs, ARG.redis_prefix)
		managedlist = getManagedGPUs(rs, ARG.redis_prefix)
		if ID in idlelist:
			print("GPU %d is already idle."%(ID))
			return 1
		if ID not in managedlist:
			print("GPU %d is not managed."%(ID))
			return 1
		if ID in managedlist and ID not in idlelist:
			TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			LOG = "%s: GPU %d released by PID %d."%(TIME, ID, PID)
			rs.lpush("%s_EVENTS"%(ARG.redis_prefix), LOG)
			rs.delete("%s_GPU%d"%(ARG.redis_prefix, ID))
			rs.lpush("%s_IDLE"%(ARG.redis_prefix), ID)
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
		print("All Redis objects related to GPU management were deleted.")
		return 0
	
	if ARG.redis_purge:
		activeList = getActiveGPUs(rs, ARG.redis_prefix)
		for i in activeList:
			PID = rs.hget("%s_GPU%d"%(ARG.redis_prefix, int(i)), "PID")
			if PID is not None:
				PID = int(PID)
				if not checkPID(PID):
					rs.delete("%s_GPU%d"%(ARG.redis_prefix, int(i)))
					LOG = "%s: GPU %s acquired by process with PID %d, was released by redis purge as the process with given PID does no longer exist."%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), i, PID)
					rs.lpush("%s_EVENTS"%(ARG.redis_prefix), LOG)
					rs.lpush("%s_IDLE"%(ARG.redis_prefix), i)
		return 0
	
	if ARG.active:
		active = getActiveGPUs(rs, ARG.redis_prefix)
		if len(active) == 0:
			print("There are no active GPUs.")
		else:
			print("There is %d ACTIVE GPUs, IDs: %s"%(len(active), ", ".join([str(i) for i in active])))
		for i in active:
			PID = rs.hget("%s_GPU%d"%(ARG.redis_prefix, int(i)), "PID")
			TIME = rs.hget("%s_GPU%d"%(ARG.redis_prefix, int(i)), "TIME")
			if TIME is not None and PID is not None:
				PID = int(PID)
				if checkPID(PID):
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
		print(f"Error: {e}")
		return -1

if __name__ == "__main__":
	sys.exit(main())
