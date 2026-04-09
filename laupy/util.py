#!/usr/bin/env python

# Utility functions

import psutil
from termcolor import colored

def list_processes(pattern="kct-", skip_pid=None):
    """
    Returns a list of dictionaries for matching processes:
    [{"pid": pid, "cmdline": ..., "cwd": ...}, ...]
    """
    import psutil, os
    skip_pid = skip_pid or os.getpid()
    procs = []

    for proc in psutil.process_iter(["pid", "ppid", "name", "cmdline", "cwd"]):
        try:
            if (proc.info["name"].startswith(pattern) 
                and proc.info["pid"] != skip_pid 
                and proc.info["ppid"] != skip_pid):
                procs.append(proc.info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return procs

def get_process_info(pid: int) -> dict:
    """
    Returns information about a process with the given PID.
    Keys: pid, ppid, name, cmdline, cwd
    """
    try:
        proc = psutil.Process(pid)
        info = {
            "pid": proc.pid,
            "ppid": proc.ppid(),
            "name": proc.name(),
            "cmdline": proc.cmdline(),
            "cwd": None,
        }
        try:
            info["cwd"] = proc.cwd()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            info["cwd"] = "[not accessible]"
        return info
    except psutil.NoSuchProcess:
        return {"pid": pid, "error": "Process does not exist"}


def print_process_info(proc_info):
    cmdline = " ".join(proc_info.get("cmdline") or ["[no cmdline]"])
    cwd = proc_info.get("cwd") or "[not accessible]"
    pid = proc_info.get("pid")
    
    print(colored("------------------------", "green", attrs=["bold"]))
    print(colored(f"Command: {cmdline}", "yellow", attrs=["bold"]))
    print(colored(f"PID: {pid}, CWD: {cwd}", "cyan"))
