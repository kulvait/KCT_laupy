#!/usr/bin/env python
# Author: Vojtech Kulvait
# Description: Redis lifecycle manager for local GPU scheduling
# Date: 2026
# License: GNU GPLv3
"""REDIS start|stop|restart|shutdown|status: command line of laupy.redis_server (lifecycle of a temporary Redis)."""

import argparse
import logging
import subprocess
import sys

from laupy.redis_server import RedisManager

# Warnings of laupy.redis_server (e.g. a stale PID file, SIGKILL after SIGTERM) go to the console
log = logging.getLogger("laupy.redis_server")
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

def main():
	parser = argparse.ArgumentParser(description="Local hardened Redis lifecycle manager for laupy")
	subparsers = parser.add_subparsers(dest="action", required=True)
	for name in ["start", "stop", "restart", "shutdown", "status"]:
		sub = subparsers.add_parser(name, help=f"{name} the Redis instance")
		sub.add_argument("--host", default="127.0.0.1", help="Redis host, default 127.0.0.1")
		sub.add_argument("--port", type=int, default=6379)
		sub.add_argument("--redis-server", default="redis-server", help="Path/name of redis-server binary")
		sub.add_argument("--pidfile", help="Path to PID file; default /tmp/laupy-gpu-<uid>-<SLURM job or local>-<port>.pid")
		sub.add_argument("--no-log", action="store_true", help="Do not write a Redis log (default: timestamped log in --log-dir)")
		sub.add_argument("--log-dir", default="log")
		# restart runs stop() and start(), so it needs the options of both
		if name == "start":
			sub.add_argument("--dry-run", action="store_true", help="Only print the configuration (executable, endpoint, PID file, log file) and exit")
		if name in ("start", "restart"):
			sub.add_argument("--extra-config", nargs=argparse.REMAINDER, default=[], help="Additional Redis options, placed after --extra-config",)
			sub.add_argument("--kill-unresponsive-server", action="store_true", help="If redis can not start because of the instance which does not respond to pings, kill it and start a new one. Use with caution.",)
		if name in ("stop", "shutdown", "restart"):
			sub.add_argument("--dump", action="store_true", help="Dump the Redis database to RDB file before stopping.")
			sub.add_argument("--force", action="store_true", help="Force stop even if PID file does not match the Redis server on the port.")
	ARG = parser.parse_args()
	if not 1 <= ARG.port <= 65535:
		parser.error("--port must be between 1 and 65535")
	try:
		# Inside the try: a missing redis-server binary is reported like any other error
		manager = RedisManager(host=ARG.host, port=ARG.port, redis_server=ARG.redis_server, pidfile=ARG.pidfile,
		                       log_dir=ARG.log_dir, no_log=ARG.no_log)
		if ARG.action == "start":
			manager.start(kill_unresponsive=ARG.kill_unresponsive_server, extra_config=ARG.extra_config,
			              dry_run=ARG.dry_run)
		elif ARG.action in ("stop", "shutdown"):
			manager.stop(force=ARG.force, dump=ARG.dump)
		elif ARG.action == "restart":
			manager.restart(kill_unresponsive=ARG.kill_unresponsive_server, extra_config=ARG.extra_config,
			                force=ARG.force, dump=ARG.dump)
		elif ARG.action == "status":
			manager.status()
	except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
		print(f"ERROR: {exc}", file=sys.stderr)
		return 1
	return 0

if __name__ == "__main__":
	sys.exit(main())
