#!/usr/bin/env python
# Author: Vojtech Kulvait
# Description: Redis lifecycle manager for local GPU scheduling
# Date: 2026
# License: GNU GPLv3
"""Lifecycle management for a temporary Redis instance: start, stop, restart, status.

Used by the REDIS command (laupy/scripts/REDIS.py) and by GPU --try-start-server, so neither depends on the
other being on PATH. RedisManager reports through out (print by default; GPU passes a function writing to
stderr, as its stdout carries only results). Problems of the program are logged through the logger of this
module; the module configures no handlers, the calling command decides where log messages go.
"""

import logging
import os
import pwd
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path

import psutil
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

log = logging.getLogger(__name__)

class RedisLifecycleError(RuntimeError):
	"""The Redis server can not be started, stopped or inspected; the message says why."""
	pass

def user_name(uid):
	"""User name of a numeric UID; the number itself if the user database (passwd, LDAP/sssd) has no entry."""
	if uid is None:
		return "unknown"
	try:
		return pwd.getpwuid(uid).pw_name
	except KeyError:
		return str(uid)

def port_owners(port):
	"""Return process details for listeners on a TCP port.

	Uses psutil (no external tool such as ss needed). As with ss, listening processes of other
	users have no visible PID and are not listed.
	"""
	try:
		connections = psutil.net_connections(kind="tcp")
	except (psutil.Error, OSError) as e:
		raise RedisLifecycleError(f"Cannot inspect port {port}: {e}")
	owners = {}
	for connection in connections:
		if connection.status != psutil.CONN_LISTEN or not connection.laddr or connection.laddr.port != port:
			continue
		pid = connection.pid
		if pid is None or pid in owners:
			continue	# Other user's process (not visible), or already listed (IPv4 and IPv6 socket)
		try:
			process = psutil.Process(pid)
			uid = process.uids().effective
			comm = process.name()
			command = " ".join(process.cmdline())
		except (psutil.Error, OSError):
			uid, comm, command = None, "unknown", ""
		owners[pid] = {
			"uid": uid,
			"comm": comm,
			"command": command,
		}
	return owners

# Limits for the quick checks of a Redis server (is it there, does it answer, what is its configuration)
PROBE_CONNECT_TIMEOUT = 1	# Seconds to establish the TCP connection
PROBE_TIMEOUT = 3	# Seconds to wait for each reply
SAVE_TIMEOUT = 60	# Seconds to wait for SAVE (stop --dump), which blocks until the RDB file is written

def probe_client(host, port, **client_options):
	"""Redis client for quick checks of a server that may be hung; every call fails after PROBE_TIMEOUT seconds.

	A hung Redis (e.g. stopped with SIGSTOP, or stuck) still lets the kernel accept TCP connections, so
	connecting succeeds and only the reply never comes; socket_timeout bounds that wait. Retries are
	switched off because redis-py >= 6 retries timed-out commands by default (3 times with growing pauses,
	about 38 s per call instead of 3 s), which made status and start appear to hang on such a server.
	Only for checks: a client that waits for work (e.g. BRPOP for a GPU) must not use these timeouts.
	client_options: further redis.Redis arguments, e.g. db or decode_responses.
	"""
	return redis.Redis(host=host, port=port, socket_connect_timeout=PROBE_CONNECT_TIMEOUT,
	                   socket_timeout=PROBE_TIMEOUT, retry=Retry(NoBackoff(), 0), **client_options)

def ping(host, port):
	"""Check if a Redis instance is responsive (False after at most PROBE_TIMEOUT seconds)."""
	try:
		r = probe_client(host, port)
		return r.ping()
	except (redis.ConnectionError, redis.TimeoutError):
		return False

def port_accepts_connections(host, port):
	"""True if something accepts TCP connections on host:port (whoever owns it)."""
	try:
		with socket.create_connection((host, port), timeout=PROBE_CONNECT_TIMEOUT):
			return True
	except OSError:
		return False

def describe_port(host, port, verbose=False, out=print):
	"""Report listeners, ownership, and Redis responsiveness of host:port.

	port_state: "free", "redis" (a responsive Redis listens) or "occupied". owner_unknown is True when
	something listens but its process is not visible (another user's process: without root, neither
	psutil nor ss shows its PID); a connection test tells such a port from a free one.
	"""
	description = {}
	description["port"] = port
	description["owners"] = port_owners(port)
	description["redis_on_port"] = False
	description["redis_is_responsive"] = False
	description["owner_unknown"] = False
	if description["owners"] == {}:
		if not port_accepts_connections(host, port):
			description["port_state"] = "free"
			out(f"Port {port}: no identifiable listening process.")
			return description
		# Occupied by a process we can not see (another user's); a responsive Redis there is usable
		description["owner_unknown"] = True
		description["redis_is_responsive"] = ping(host, port)
		description["redis_on_port"] = description["redis_is_responsive"]
		if description["redis_is_responsive"]:
			description["redis_pid"] = server_pid(host, port)
			description["port_state"] = "redis"
			out(f"Port {port}: responsive Redis of another user (PID {description['redis_pid']}).")
		else:
			description["port_state"] = "occupied"
			out(f"Port {port}: occupied by a process of another user (owner not visible), not a responsive Redis.")
		return description
	description["port_state"] = "occupied"
	for pid, info in description["owners"].items():
		owner = user_name(info["uid"])
		description["owners"][pid]["owner"] = owner
		description["owners"][pid]["info"] = info
		description["owners"][pid]["is_redis"] = (info["comm"] == "redis-server")
		if description["owners"][pid]["is_redis"]:
			description["redis_pid"] = pid
			# Try to query pidfile (probe client: a hung server gives up after PROBE_TIMEOUT)
			rs = probe_client(host, port)
			try:
				pidfile = rs.config_get("pidfile").get("pidfile", None)
				description["owners"][pid]["pidfile"] = pidfile
				description["redis_pidfile"] = pidfile
			except (redis.ConnectionError, redis.TimeoutError):
				description["owners"][pid]["pidfile"] = None
			description["owners"][pid]["is_responsive_redis"] = ping(host, port)
		else:
			description["owners"][pid]["is_responsive_redis"] = False
		description["redis_on_port"] |= description["owners"][pid]["is_redis"]
		description["redis_is_responsive"] |= description["owners"][pid]["is_responsive_redis"]
		if verbose:
			out(f"Port {port}: PID {pid}, owner={owner}, process={info['comm']}, responsive={'yes' if description['owners'][pid]['is_responsive_redis'] else 'no'}, pidfile={description['owners'][pid].get('pidfile', 'unknown')}")
		if description["redis_on_port"] and description["redis_is_responsive"]:
			description["port_state"] = "redis"
		else:
			description["port_state"] = "occupied"
	return description

def find_executable(name_or_path: str) -> str:
	if "/" in name_or_path:
		p = Path(name_or_path).expanduser().resolve()
		if not p.is_file() or not os.access(p, os.X_OK):
			raise RedisLifecycleError(f"Not executable: {p}")
		return str(p)
	p = shutil.which(name_or_path)
	if p is None:
		raise RedisLifecycleError(f"Executable not found in PATH: {name_or_path}")
	return str(Path(p).resolve())

def pidfile_pid(pidfile):
	try:
		return int(pidfile.read_text().strip())
	except (OSError, ValueError):
		return None

def process_exists(pid):
	if pid is None or pid <= 0:
		return False
	try:
		os.kill(pid, 0)
		return True
	except ProcessLookupError:
		return False
	except PermissionError:
		return True

def server_pid(host, port):
	"""Return the Redis server PID reported by the listening instance."""
	try:
		with socket.create_connection((host, port), timeout=1) as sock:
			sock.sendall(b"INFO server\r\n")
			response = sock.recv(65536).decode(errors="replace")
		for line in response.splitlines():
			if line.startswith("process_id:"):
				return int(line.split(":", 1)[1])
	except (OSError, ValueError):
		pass
	return None

def proc_identity(pid):
	"""Return a string identifying the process by PID."""
	try:
		comm = Path(f"/proc/{pid}/comm").read_text().strip()
		uid = os.stat(f"/proc/{pid}").st_uid
		return f"{comm} (PID {pid}, user {user_name(uid)})"
	except OSError:
		return f"unknown (PID {pid})"

TERMINATE_TIMEOUT = 5	# Seconds to wait after SIGTERM before SIGKILL, and after SIGKILL before giving up

def is_own_redis_server(pid):
	"""True if PID is a redis-server process of the current user (checked right before signalling it,
	so a PID that was reused meanwhile by another process is never killed)."""
	try:
		process = psutil.Process(pid)
		return process.name() == "redis-server" and process.uids().effective == os.getuid()
	except (psutil.Error, OSError):
		return False

def terminate(pid):
	"""Terminate a process: SIGTERM, and SIGKILL if it is still there after TERMINATE_TIMEOUT seconds.

	SIGTERM lets Redis shut down cleanly, but a hung or stopped (SIGSTOP) server does not act on it;
	SIGKILL cannot be ignored. Callers must have verified the process (is_own_redis_server()).
	Returns True when the process is gone.
	"""
	for sig in (signal.SIGTERM, signal.SIGKILL):
		try:
			os.kill(pid, sig)
		except ProcessLookupError:
			return True
		except OSError:
			return False
		deadline = time.monotonic() + TERMINATE_TIMEOUT
		while time.monotonic() < deadline:
			if not process_exists(pid):
				return True
			time.sleep(0.1)
		if sig == signal.SIGTERM:
			log.warning(f"PID {pid} did not exit within {TERMINATE_TIMEOUT} s after SIGTERM, sending SIGKILL.")
	return False

class RedisManager:
	"""A Redis instance on host:port, identified by its PID file.

	host, port: endpoint (the server binds host). redis_server: name or path of the redis-server binary.
	pidfile: default /tmp/laupy-gpu-<uid>-<SLURM job or local>-<port>.pid. log_dir: directory of the
	timestamped log and of stop(dump=True) files, relative to the working directory. no_log: write no log.
	out: function receiving the messages (print by default). Failures raise RedisLifecycleError.
	"""

	def __init__(self, host="127.0.0.1", port=6379, redis_server="redis-server", pidfile=None, log_dir="log",
	             no_log=False, out=print):
		self.host = host
		self.port = port
		self.out = out
		self.redis_server_exe = find_executable(redis_server)
		# File names carry the SLURM job and the port, so instances of different jobs on one node do not collide
		job_id = os.environ.get("SLURM_JOB_ID", "local")
		self.job_id = job_id
		# PID file in /tmp: node-local, keeps the work directory clean, and other instances can look for
		# laupy-gpu-* files there; the UID separates users
		if pidfile:
			self.pidfile = Path(pidfile).resolve()
		else:
			self.pidfile = Path("/tmp") / f"laupy-gpu-{os.getuid()}-{job_id}-{self.port}.pid"
		# Log and dump are timestamped: files of several runs and nodes may end up in the same log dir
		self.log_dir = Path(log_dir).resolve()
		timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
		self.logfile = None if no_log else self.log_dir / f"laupy-gpu-{timestamp}-{job_id}-{self.port}.log"
		self.dump_rdb = self.log_dir / f"laupy-gpu-{timestamp}-{job_id}-{self.port}.rdb"

	def status(self):
		# Figure out if PID file exists and what it points to.
		pidfile_value = pidfile_pid(self.pidfile)
		description = describe_port(self.host, self.port, out=self.out)
		if description["port_state"] == "free":
			self.out(f"Redis is not running on {self.host}:{self.port}")
			if pidfile_value is not None:
				if process_exists(pidfile_value):
					log.warning(f"PID file {self.pidfile} exists and points to PID {pidfile_value}, but no process is listening on the port {self.port}.")
				else:
					log.warning(f"PID file {self.pidfile} exists and points to PID {pidfile_value}, but no process with such PID is running. This may indicate a stale PID file.")
		elif description["redis_on_port"]:
			process_pid = description.get("redis_pid", None)
			process_pidfile = description.get("redis_pidfile", None)
			if description["owner_unknown"]:
				process_executable = "not visible (another user's process)"
			else:
				process_executable = Path(f"/proc/{process_pid}/exe").resolve() if process_pid else None
			if description["redis_is_responsive"]:
				self.out(f"{self.host}:{self.port} - responsive Redis server PID {process_pid}, path: {process_executable}.")
			else:
				self.out(f"{self.host}:{self.port} - unresponsive Redis server PID {process_pid}, path: {process_executable}.")
			if process_pidfile:
				process_pidfile_value = pidfile_pid(Path(process_pidfile))
				if process_pidfile_value is not None and process_pidfile_value != process_pid:
					log.warning(f"PID file {process_pidfile} points to PID {process_pidfile_value}, but the Redis server on port {self.port} reports PID {process_pid}. This may indicate a stale or incorrect PID file.")
				if pidfile_value is not None:
					if process_pid != pidfile_value:
						log.warning(f"Managed PID file {self.pidfile} points to PID {pidfile_value}, but the Redis server on port {self.port} reports PID {process_pid}. This may indicate a configuration mismatch or multiple Redis instances.")
					if process_pidfile is not None and self.pidfile.resolve() != Path(process_pidfile).resolve():
						log.warning(f"Managed PID file {self.pidfile} does not match the Redis server's PID file {process_pidfile}. This may indicate a configuration mismatch or multiple Redis instances.")
					if process_pidfile is None:
						log.warning(f"Redis server on port {self.port} does not report a PID file, but the managed PID file {self.pidfile} exists. This may indicate a configuration mismatch or multiple Redis instances.")
		elif description["owner_unknown"]:
			self.out(f"{self.host}:{self.port} - port is occupied by a process of another user (not visible), "
			         "which is not a responsive Redis.")
		else:
			self.out(f"{self.host}:{self.port} - port is occupied by non-Redis processes:")
			for pid in description["owners"]:
				self.out(f"  {proc_identity(pid)}")
		if pidfile_value is not None:
			if process_exists(pidfile_value):
				self.out(f"Managed PID file exists: {self.pidfile} -> PID {pidfile_value} (running)")
			else:
				self.out(f"Stale PID file: {self.pidfile} -> PID {pidfile_value} (not running)")
		if description["redis_is_responsive"]:
			# The log name is timestamped at start, so ask the running server which log it writes
			try:
				logfile = probe_client(self.host, self.port).config_get("logfile").get("logfile")
				self.out(f"Log file: {logfile}" if logfile else "Log file: none (started with --no-log)")
			except (redis.ConnectionError, redis.TimeoutError):
				pass

	def start(self, kill_unresponsive=False, extra_config=(), dry_run=False):
		"""Start the server unless a responsive Redis already runs on the port.

		kill_unresponsive: free a port held by your own redis-server that does not respond (SIGTERM, then SIGKILL).
		extra_config: additional redis-server options. dry_run: only report the configuration and the files.
		"""
		self.out(f"Redis executable: {self.redis_server_exe}")
		self.out(f"Redis endpoint:   {self.host}:{self.port}")
		if dry_run:
			self.out("Without --dry-run, start would create these files:")
		self.out(f"PID file:		  {self.pidfile}")
		if self.logfile:
			self.out(f"Log file:		  {self.logfile}")
		else:
			self.out("Log file:		  none (--no-log)")
		if dry_run:
			# The dump name is timestamped when stop --dump runs, so only its pattern is known here
			self.out(f"Dump file:		  {self.log_dir}/laupy-gpu-<time of stop>-{self.job_id}-{self.port}.rdb "
			         "(written only by stop --dump)")
			return
		self.pidfile.parent.mkdir(parents=True, exist_ok=True)
		if self.logfile:
			self.logfile.parent.mkdir(parents=True, exist_ok=True)
		description = describe_port(self.host, self.port, out=self.out)
		if description["port_state"] == "redis":
			self.out(f"Redis is already running on port {self.port}; skipping start.")
			return
		if description["port_state"] == "occupied":
			owners = description["owners"]
			if description["owner_unknown"]:
				# Nothing we may kill, and a server started now could not bind the port
				raise RedisLifecycleError(f"Port {self.port} is held by a process of another user (not visible), "
				                          "which is not a responsive Redis. Use another --port.")
			if not kill_unresponsive:
				raise RedisLifecycleError(f"Port {self.port} is occupied. Use status to inspect the instance, or --kill-unresponsive-server to permit recovery.")
			for pid, info in owners.items():
				if info["uid"] != os.getuid():
					raise RedisLifecycleError(
						f"Port {self.port} is held by PID {pid}, "
						"which is not owned by the current user. "
						"Refusing to kill it."
					)
				if info["comm"] != "redis-server":
					raise RedisLifecycleError(
						f"Port {self.port} is held by {info['comm']} "
						f"(PID {pid}), not Redis. Refusing to kill it."
					)
				if not is_own_redis_server(pid):
					raise RedisLifecycleError(
						f"Cannot verify {proc_identity(pid)} as your redis-server; refusing to kill it."
					)
				if not terminate(pid):
					raise RedisLifecycleError(f"Could not terminate PID {pid}.")
		existing_pid = pidfile_pid(self.pidfile)
		if existing_pid and process_exists(existing_pid):
			if server_pid(self.host, self.port) == existing_pid:
				self.out(f"Redis is already running (PID {existing_pid}).")
				return
			raise RedisLifecycleError(
				f"PID file refers to PID {existing_pid}, "
				"but it does not match the Redis server on this port. "
				"Check for a stale PID file or port conflict."
			)
		# Remove a stale PID file.
		self.pidfile.unlink(missing_ok=True)
		# No old state can be loaded: persistence is off (--save "", --appendonly no) and --dbfilename is
		# timestamped, so Redis never finds an existing file of that name in its working directory.
		cmd = [
			self.redis_server_exe,	# The resolved binary that start() reports
			"--bind", self.host,
			"--port", str(self.port),
			"--daemonize", "yes",
			"--pidfile", str(self.pidfile),
			"--save", "",
			"--appendonly", "no",
		]
		if self.dump_rdb:
			cmd += ["--dbfilename", str(self.dump_rdb.name)]
		if self.logfile:
			cmd += ["--logfile", str(self.logfile)]
		else:
			cmd += ["--logfile", ""]
		if extra_config:
			cmd += list(extra_config)
		result = subprocess.run(
			cmd, capture_output=True, text=True, timeout=10
		)
		if result.returncode:
			raise RedisLifecycleError(
				f"redis-server failed:\n{result.stderr}\n{result.stdout}"
			)
		deadline = time.monotonic() + 10
		while time.monotonic() < deadline:
			pid = pidfile_pid(self.pidfile)
			if pid and server_pid(self.host, self.port) == pid:
				self.out(f"Redis started (PID {pid}, port {self.port}).")
				if self.logfile:
					self.out(f"Logs: {self.logfile}")
				return
			time.sleep(0.2)
		raise RedisLifecycleError(
			"Redis did not become ready within 10 seconds. "
			"Check the Redis log and port availability."
		)

	def stop(self, force=False, dump=False, force_message="Force stopping Redis (ignoring PID file)."):
		"""Stop the server of the PID file (SHUTDOWN NOSAVE).

		force: ignore the PID file and stop your own redis-server on the port (SIGTERM, then SIGKILL).
		dump: first save the data to the timestamped RDB file in the log dir.
		force_message: first line reported for a forced stop (callers can say why they stop it).
		"""
		if force:
			self.out(force_message)
			owners = port_owners(self.port)
			for pid, info in owners.items():
				if is_own_redis_server(pid):
					if terminate(pid):
						self.out(f"Terminated Redis PID {pid}.")
					else:
						self.out(f"Failed to terminate Redis PID {pid}.")
			self.pidfile.unlink(missing_ok=True)
			return
		pid = pidfile_pid(self.pidfile)
		if pid is None:
			self.out("No managed Redis PID file found; nothing to stop.")
			return
		if not process_exists(pid):
			self.pidfile.unlink(missing_ok=True)
			self.out("Removed stale PID file.")
			return
		actual_pid = server_pid(self.host, self.port)
		if actual_pid is None:
			raise RedisLifecycleError(
				f"Redis on {self.host}:{self.port} does not respond (its PID {pid} from {self.pidfile} is "
				"still running). Use stop --force to kill it."
			)
		if actual_pid != pid:
			raise RedisLifecycleError(
				f"Refusing to stop: PID file says {pid}, "
				f"but Redis on {self.host}:{self.port} reports "
				f"{actual_pid}. Check the port and PID file."
			)
		if dump:
			# SAVE writes <dir>/<dbfilename> of the running server (dir is its working directory, it can not
			# be changed at runtime in Redis >= 7), so ask where that is and move the file to the log dir.
			try:
				rs = probe_client(self.host, self.port)
				saved = Path(rs.config_get("dir")["dir"]) / rs.config_get("dbfilename")["dbfilename"]
				self.out(f"Dumping Redis database to {self.dump_rdb} before shutdown.")
				# SAVE blocks until the file is written, which takes longer than a probe on a big database
				redis.Redis(host=self.host, port=self.port, socket_connect_timeout=PROBE_CONNECT_TIMEOUT,
				            socket_timeout=SAVE_TIMEOUT, retry=Retry(NoBackoff(), 0)).save()
			except redis.RedisError as e:
				raise RedisLifecycleError(f"SAVE failed, Redis not stopped: {e}")
			self.dump_rdb.parent.mkdir(parents=True, exist_ok=True)
			shutil.move(str(saved), str(self.dump_rdb))
		self.out(f"Stopping Redis (PID {pid}) on {self.host}:{self.port} ...")
		try:
			# NOSAVE prevents a final RDB save during shutdown. redis-py returns once the server closed
			# the connection, and raises if the server refused or did not answer.
			probe_client(self.host, self.port).shutdown(nosave=True)
		except redis.RedisError as e:
			raise RedisLifecycleError(f"SHUTDOWN failed: {e}. Use stop --force to kill the server.")
		deadline = time.monotonic() + 10
		while time.monotonic() < deadline:
			if not process_exists(pid):
				self.pidfile.unlink(missing_ok=True)
				self.out(f"Redis stopped (PID {pid}).")
				return
			time.sleep(0.2)
		raise RedisLifecycleError(
			f"Redis PID {pid} did not exit within 10 seconds."
		)

	def restart(self, kill_unresponsive=False, extra_config=(), force=False, dump=False):
		self.stop(force=force, dump=dump)
		self.start(kill_unresponsive=kill_unresponsive, extra_config=extra_config)

	def shutdown(self, force=False, dump=False):
		self.stop(force=force, dump=dump)
