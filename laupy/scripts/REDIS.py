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
import time
from pathlib import Path

class RedisLifecycleError(RuntimeError):
	pass


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
	return Path(log_dir) / (f"laupy-gpu-{os.getuid()}-{job_id}-{port}.pid")

def redis_pid(pidfile):
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

class RedisManager:
	def __init__(self, ARG):
		self.ARG = ARG
		self.host = ARG.host
		self.port = ARG.port
		self.pidfile = Path(ARG.pidfile).resolve()
		self.logfile = None
		self.redis_server_exe = find_executable(ARG.redis_server)

	def init(self):
		"""Check prerequisites and prepare runtime directories."""
		binary = self.redis_server_exe
		self.pidfile.parent.mkdir(parents=True, exist_ok=True)
		if self.ARG.log:
			log_dir = Path(self.ARG.log_dir).resolve()
			log_dir.mkdir(parents=True, exist_ok=True)
			self.logfile = log_dir / (f"laupy_gpu_{os.getpid()}.log")

		print(f"Redis executable: {binary}")
		print(f"Redis endpoint:   {self.host}:{self.port}")
		print(f"PID file:		  {self.pidfile}")
		if self.logfile:
			print(f"Log file:		  {self.logfile}")

	def start(self):
		self.init()
		existing_pid = redis_pid(self.pidfile)
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

		cmd = [
			self.ARG.redis_server,
			"--bind", self.host,
			"--port", str(self.port),
			"--daemonize", "yes",
			"--pidfile", str(self.pidfile),
			"--save", "",
			"--appendonly", "no",
		]

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
			pid = redis_pid(self.pidfile)
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
		pid = redis_pid(self.pidfile)

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
	parser.add_argument("action", choices=["init", "start", "stop", "restart", "shutdown"], )
	parser.add_argument("--host", default="127.0.0.1", help="Redis host, default 127.0.0.1")
	parser.add_argument("--port", type=int, default=6379)
	parser.add_argument("--redis-server", default="redis-server", help="Path/name of redis-server binary")
	parser.add_argument("--redis-cli", default="redis-cli")
	parser.add_argument("--pidfile", help="Path to PID file; default is log/laupy-gpu-<uid>-<jobid>-<port>.pid")
	parser.add_argument("--log", action="store_true")
	parser.add_argument("--log-dir", default="log")
	parser.add_argument("--extra-config", nargs=argparse.REMAINDER, default=[], help="Additional Redis options, placed after --extra-config",)
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
	except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
		print(f"ERROR: {exc}", file=sys.stderr)
		return 1
	return 0

if __name__ == "__main__":
	sys.exit(main())

