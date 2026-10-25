#!/usr/bin/env python
# Author: Vojtech Kulvait
# Description: Redis lifecycle manager for local GPU scheduling
# Date: 2026
# License: GNU GPLv3
"""Lifecycle management for a temporary Redis instance."""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import pwd
import re
import signal
import time
import logging
from pathlib import Path

import redis

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

class RedisLifecycleError(RuntimeError):
	pass

def port_owners(port):
	"""Return process details for listeners on a TCP port."""
	result = subprocess.run(
		["ss", "-H", "-ltnp", f"sport = :{port}"],
		capture_output=True,
		text=True,
		timeout=3,
	)
	if result.returncode != 0:
		raise RuntimeError(
			f"Cannot inspect port {port}: {result.stderr.strip()}"
		)
	owners = {}
	for line in result.stdout.splitlines():
		for match in re.finditer(r"pid=(\d+)", line):
			pid = int(match.group(1))
			try:
				uid = os.stat(f"/proc/{pid}").st_uid
				comm = Path(f"/proc/{pid}/comm").read_text().strip()
			except OSError:
				uid, comm = None, "unknown"
			owners[pid] = {
				"uid": uid,
				"comm": comm,
				"command": line.strip(),
			}
	return owners

def ping(host, port):
	"""Check if a Redis instance is responsive."""
	try:
		r = redis.Redis(host=host, port=port, socket_connect_timeout=1)
		return r.ping()
	except (redis.ConnectionError, redis.TimeoutError):
		return False

def describe_port(port, redis_cli, verbose=False):
	"""Report listeners, ownership, and Redis responsiveness."""
	description = {}
	description["port"] = port
	description["owners"] = port_owners(port)
	description["redis_on_port"] = False
	description["redis_is_responsive"] = False
	if description["owners"] == {}:
		description["port_state"] = "free"
		print(f"Port {port}: no identifiable listening process.")
		return description
	description["port_state"] = "occupied"
	for pid, info in description["owners"].items():
		owner = (
			pwd.getpwuid(info["uid"]).pw_name
			if info["uid"] is not None
			else "unknown"
		)
		description["owners"][pid]["owner"] = owner
		description["owners"][pid]["info"] = info
		description["owners"][pid]["is_redis"] = (info["comm"] == "redis-server")
		if description["owners"][pid]["is_redis"]:
			description["redis_pid"] = pid
			# Try to query pidfile
			rs = redis.Redis(host="127.0.0.1", port=port, socket_connect_timeout=1)
			try:
				pidfile = rs.config_get("pidfile").get("pidfile", None)
				description["owners"][pid]["pidfile"] = pidfile
				description["redis_pidfile"] = pidfile
			except (redis.ConnectionError, redis.TimeoutError):
				description["owners"][pid]["pidfile"] = None
			description["owners"][pid]["is_responsive_redis"] = ping("127.0.0.1", port)
		else:
			description["owners"][pid]["is_responsive_redis"] = False
		description["redis_on_port"] |= description["owners"][pid]["is_redis"]
		description["redis_is_responsive"] |= description["owners"][pid]["is_responsive_redis"]
		if verbose:
			print(f"Port {port}: PID {pid}, owner={owner}, process={info['comm']}, responsive={'yes' if responsive else 'no'}")
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

def default_pidfile(port, log_dir):
	# Isolate instances belonging to different Slurm jobs.
	job_id = os.environ.get("SLURM_JOB_ID", "local")
	return Path("/tmp") / (f"laupy-gpu-{os.getuid()}-{job_id}-{port}.pid")

def default_logfile(port, log_dir):
	job_id = os.environ.get("SLURM_JOB_ID", "local")
	timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
	return Path(log_dir) / (f"laupy-gpu-{timestamp}-{job_id}-{port}.log")

def default_dump_rdb(port, log_dir):
	job_id = os.environ.get("SLURM_JOB_ID", "local")
	timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
	return Path(log_dir) / (f"laupy-gpu-{timestamp}-{job_id}-{port}.rdb")

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
		user = pwd.getpwuid(uid).pw_name
		return f"{comm} (PID {pid}, user {user})"
	except OSError:
		return f"unknown (PID {pid})"

def terminate(pid):
	"""Attempt to terminate a process by PID."""
	try:
		os.kill(pid, signal.SIGTERM)
	except OSError:
		return False
	deadline = time.monotonic() + 5
	while time.monotonic() < deadline:
		if not process_exists(pid):
			return True
		time.sleep(0.1)
	return False

class RedisManager:
	def __init__(self, ARG):
		self.ARG = ARG
		self.host = ARG.host
		self.port = ARG.port
		self.pidfile = Path(ARG.pidfile).resolve()
		self.logfile = None
		self.redis_server_exe = find_executable(ARG.redis_server)
		self.log_dir = Path(self.ARG.log_dir).resolve()
		self.logfile = default_logfile(self.port, self.log_dir)

	def init(self):
		"""Check prerequisites and prepare runtime directories."""
		self.pidfile.parent.mkdir(parents=True, exist_ok=True)
		self.dump_rdb = default_dump_rdb(self.port, self.ARG.log_dir)
		self.dump_rdb.parent.mkdir(parents=True, exist_ok=True)
		if self.ARG.log:
			log_dir = Path(self.ARG.log_dir).resolve()
			log_dir.mkdir(parents=True, exist_ok=True)
			self.logfile = default_logfile(self.port, log_dir)
		print(f"Redis executable: {self.redis_server_exe}")
		print(f"Redis endpoint:   {self.host}:{self.port}")
		print(f"PID file:		  {self.pidfile}")
		if self.logfile:
			print(f"Log file:		  {self.logfile}")

	def status(self):
		# Figure out if PID file exists and what it points to.
		pidfile_value = pidfile_pid(self.pidfile)
		description = describe_port(self.port, self.ARG.redis_cli)
		if description["port_state"] == "free":
			print(f"Redis is not running on 127.0.0.1:{self.port}")
			if pidfile_value is not None:
				if process_exists(pidfile_value):
					log.warning(f"PID file {self.pidfile} exists and points to PID {pidfile_value}, but no process is listening on the port {self.port}.")
				else:
					log.warning(f"PID file {self.pidfile} exists and points to PID {pidfile_value}, but no process with such PID is running. This may indicate a stale PID file.")
		elif description["redis_on_port"]:
			process_pid = description.get("redis_pid", None)
			process_pidfile = description.get("redis_pidfile", None)
			process_executable = Path(f"/proc/{process_pid}/exe").resolve() if process_pid else None
			if description["redis_is_responsive"]:
				print(f"127.0.0.1:{self.port} - responsive Redis server PID {process_pid}, path: {process_executable}.")
			else:
				print(f"127.0.0.1:{self.port} - unresponsive Redis server PID {process_pid}, path: {process_executable}.")
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
		else:
			print(f"127.0.0.1:{self.port} - port is occupied by non-Redis processes:")
		if pidfile_value is not None:
			if process_exists(pidfile_pid):
				print(f"Managed PID file exists: {self.pidfile} -> PID {pidfile_value} (running)")
			else:
				print(f"Stale PID file: {self.pidfile} -> PID {pidfile_value} (not running)")
		if self.logfile and self.logfile.exists():
			print(f"Log file exists: {self.logfile}")

	def start(self):
		self.init()
		description = describe_port(self.port, self.ARG.redis_cli)
		if description["port_state"] == "redis":
			print(f"Redis is already running on port {self.port}; skipping start.")
			return
		if description["port_state"] == "occupied":
			owners = description["owners"]
			if not self.ARG.kill_unresponsive_server:
				raise RuntimeError(f"Port {self.port} is occupied. Use status to inspect the instance, or --kill-unresponsive-server to permit recovery.")
			for pid, info in owners.items():
				if info["uid"] != os.getuid():
					raise RuntimeError(
						f"Port {self.port} is held by PID {pid}, "
						"which is not owned by the current user. "
						"Refusing to kill it."
					)
				if info["comm"] != "redis-server":
					raise RuntimeError(
						f"Port {self.port} is held by {info['comm']} "
						f"(PID {pid}), not Redis. Refusing to kill it."
					)
				if not proc_identity(pid):
					raise RuntimeError(
						f"Cannot verify PID {pid}; refusing to kill it."
					)
				if not terminate(pid):
					raise RuntimeError(f"Could not terminate PID {pid}.")
		existing_pid = pidfile_pid(self.pidfile)
		if existing_pid and process_exists(existing_pid):
			if server_pid(self.host, self.port) == existing_pid:
				print(f"Redis is already running (PID {existing_pid}).")
				return
			raise RuntimeError(
				f"PID file refers to PID {existing_pid}, "
				"but it does not match the Redis server on this port. "
				"Check for a stale PID file or port conflict."
			)
		# Remove a stale PID file.
		self.pidfile.unlink(missing_ok=True)
		# Remove dummp.rdb if it exists, to avoid Redis loading an old state.
		dump_file = Path("dump.rdb")
		if dump_file.exists():
			print(f"Removing existing dump file: {dump_file}")
			dump_file.unlink()
		cmd = [
			self.ARG.redis_server,
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
		if self.ARG.extra_config:
			cmd += self.ARG.extra_config
		result = subprocess.run(
			cmd, capture_output=True, text=True, timeout=10
		)
		if result.returncode:
			raise RuntimeError(
				f"redis-server failed:\n{result.stderr}\n{result.stdout}"
			)
		deadline = time.monotonic() + 10
		while time.monotonic() < deadline:
			pid = pidfile_pid(self.pidfile)
			if pid and server_pid(self.host, self.port) == pid:
				print(f"Redis started (PID {pid}, port {self.port}).")
				if self.logfile:
					print(f"Logs: {self.logfile}")
				return
			time.sleep(0.2)
		raise RuntimeError(
			"Redis did not become ready within 10 seconds. "
			"Check the Redis log and port availability."
		)

	def stop(self):
		if self.ARG.force:
			print("Force stopping Redis (ignoring PID file).")
			owners = port_owners(self.port)
			for pid, info in owners.items():
				if info["comm"] == "redis-server" and info["uid"] == os.getuid():
					if terminate(pid):
						print(f"Terminated Redis PID {pid}.")
					else:
						print(f"Failed to terminate Redis PID {pid}.")
			self.pidfile.unlink(missing_ok=True)
			return
		pid = pidfile_pid(self.pidfile)
		if pid is None:
			print("No managed Redis PID file found; nothing to stop.")
			return
		if not process_exists(pid):
			self.pidfile.unlink(missing_ok=True)
			print("Removed stale PID file.")
			return
		actual_pid = server_pid(self.host, self.port)
		if actual_pid != pid:
			raise RuntimeError(
				f"Refusing to stop: PID file says {pid}, "
				f"but Redis on {self.host}:{self.port} reports "
				f"{actual_pid}. Check the port and PID file."
			)
		# NOSAVE prevents a final RDB save during shutdown.
		if self.ARG.dump:
			print(f"Dumping Redis database to {self.dump_rdb} before shutdown.")
			subprocess.run(
				[
					self.ARG.redis_cli,
					"-h", self.host,
					"-p", str(self.port),
					"SAVE",
				],
				stdout=subprocess.DEVNULL,
				stderr=subprocess.DEVNULL,
				timeout=5,
			)
		print(f"Stopping Redis (PID {pid}) on {self.host}:{self.port} ...")
		subprocess.run(
			[
				self.ARG.redis_cli,
				"-h", self.host,
				"-p", str(self.port),
				"SHUTDOWN", "NOSAVE",
			],
			stdout=subprocess.DEVNULL,
			stderr=subprocess.DEVNULL,
			timeout=5,
		)
		deadline = time.monotonic() + 10
		while time.monotonic() < deadline:
			if not process_exists(pid):
				self.pidfile.unlink(missing_ok=True)
				print(f"Redis stopped (PID {pid}).")
				return
			time.sleep(0.2)
		raise RuntimeError(
			f"Redis PID {pid} did not exit within 10 seconds."
		)

	def restart(self):
		self.stop()
		self.start()

	def shutdown(self):
		self.stop()


def main():
	parser = argparse.ArgumentParser(description="Local hardened Redis lifecycle manager for laupy")
	subparsers = parser.add_subparsers(dest="action", required=True)
	for name in ["init", "start", "stop", "restart", "shutdown", "status"]:
		sub = subparsers.add_parser(name, help=f"{name} the Redis instance")
		sub.add_argument("--host", default="127.0.0.1", help="Redis host, default 127.0.0.1")
		sub.add_argument("--port", type=int, default=6379)
		sub.add_argument("--redis-server", default="redis-server", help="Path/name of redis-server binary")
		sub.add_argument("--redis-cli", default="redis-cli")
		sub.add_argument("--pidfile", help="Path to PID file; default is log/laupy-gpu-<uid>-<jobid>-<port>.pid")
		sub.add_argument("--log", action="store_true")
		sub.add_argument("--log-dir", default="log")
		if name in ("init", "start"):
			sub.add_argument("--extra-config", nargs=argparse.REMAINDER, default=[], help="Additional Redis options, placed after --extra-config",)
			sub.add_argument("--kill-unresponsive-server", action="store_true", help="If redis can not start because of the instance which does not respond to pings, kill it and start a new one. Use with caution.",)
		if name in ("stop, shutdown"):
			sub.add_argument("--dump", action="store_true", help="Dump the Redis database to RDB file before stopping.")
			sub.add_argument("--force", action="store_true", help="Force stop even if PID file does not match the Redis server on the port.")
	ARG = parser.parse_args()
	if not 1 <= ARG.port <= 65535:
		parser.error("--port must be between 1 and 65535")
	if not ARG.pidfile:
		ARG.pidfile = str(default_pidfile(ARG.port, ARG.log_dir))
	manager = RedisManager(ARG)
	try:
		if ARG.action == "init":
			manager.init()
		elif ARG.action == "start":
			manager.start()
		elif ARG.action in ("stop", "shutdown"):
			manager.stop()
		elif ARG.action == "restart":
			manager.restart()
		elif ARG.action == "status":
			manager.status()
	except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
		print(f"ERROR: {exc}", file=sys.stderr)
		return 1
	return 0

if __name__ == "__main__":
	sys.exit(main())

