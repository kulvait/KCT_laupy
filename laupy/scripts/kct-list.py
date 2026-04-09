#!/usr/bin/env python3
import argparse
import os
from termcolor import colored
from laupy import util  # import your utility module

def main():
    # ---------------------------
    # Argument parsing
    # ---------------------------
    parser = argparse.ArgumentParser(
        description="List processes matching a name pattern with their command line and working directory."
    )
    parser.add_argument(
        "--pattern", type=str, default="kct-", help="Process name pattern to match (default: kct-)"
    )
    args = parser.parse_args()

    PATTERN = args.pattern
    SELF_PID = os.getpid()

    # ---------------------------
    # Find matching processes
    # ---------------------------
    matching_procs = util.list_processes(pattern=PATTERN, skip_pid=SELF_PID)

    # ---------------------------
    # Report
    # ---------------------------
    count = len(matching_procs)
    if count == 0:
        print(f"No processes found matching pattern '{PATTERN}'")
        return

    print(f"Found {count} process(es) matching pattern '{PATTERN}'")
    print(colored("------------------------", "green", attrs=["bold"]))

    # ---------------------------
    # Print processes
    # ---------------------------
    for proc_info in matching_procs:
        util.print_process_info(proc_info)

# Entry point for CLI
if __name__ == "__main__":
    main()
