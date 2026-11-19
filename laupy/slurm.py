#!/usr/bin/env python

# Utility functions
from typing import Union, List, Dict, Optional
import getpass
import os
import subprocess
import logging
import re


# Create a logger specific to this module
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)  # Set the logging level to INFO
# Create a console handler and set its level to INFO
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# Create a formatter and set it for the handler
formatter = logging.Formatter(
    '%(asctime)s - %(name)s:%(lineno)d - %(levelname)s : %(message)s', datefmt='%d.%m.%Y %H:%M:%S')
ch.setFormatter(formatter)
# Add the handler to the logger
log.addHandler(ch)
log.propagate = False  # Prevent log messages from being propagated to the root logger


# Job state groups shared by the pipeline CLI and lautools-browser.
ACTIVE_STATES = ("PENDING", "RUNNING", "REQUEUED", "COMPLETING")
FAILED_STATES = ("FAILED", "CANCELLED", "TIMEOUT")
# COMPLETING means the job already ended and SLURM is cleaning up; it is
# normally transient and should be cancelled only when stuck.
TRANSIENT_STATES = ("COMPLETING",)
# Final states: once stored in an entry they never change, so they need not
# be queried again. (NODE_FAIL and PREEMPTED are excluded, jobs may requeue.)
TERMINAL_STATES = ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "BOOT_FAIL", "DEADLINE")


def normalize_state(state: str) -> str:
    return state.split()[0].replace("+", "")


# Batched queries: job IDs are deduplicated and sent in chunks of this size
# (one sacct/squeue process per chunk, independent of how many entries or
# processing units they come from).
QUERY_CHUNK_SIZE = 500
# Number of sacct/squeue processes started by this module, for diagnostics.
QUERY_COUNTS = {"sacct": 0, "squeue": 0}
_SLURM_ID_RE = re.compile(r"^\d+(_\d+)?$")  # 1234 or array task 1234_5
_SACCT_FORMAT = "JobID,JobName,State,ExitCode,Partition,User,Elapsed,Timelimit,NNodes,NodeList,Reason,StdOut,StdErr"


def _as_id_list(slurm_ids):
    single = isinstance(slurm_ids, (str, int))
    return single, ([str(slurm_ids)] if single else [str(sid) for sid in slurm_ids])


def _query_lines(tool: str, slurm_ids: List[str], build_cmd) -> Dict[str, str]:
    """Run ``tool`` for the valid, unique IDs in chunks; return {JobID: first output line}.

    IDs that are not SLURM job IDs (e.g. legacy uuid job_ids) are not sent,
    since a single invalid ID makes the whole sacct call fail. A failed or
    empty chunk leaves its IDs unknown; there is no per-ID retry.
    """
    unique = list(dict.fromkeys(sid for sid in slurm_ids if _SLURM_ID_RE.match(sid)))
    lines_by_id: Dict[str, str] = {}
    for start in range(0, len(unique), QUERY_CHUNK_SIZE):
        chunk = unique[start:start + QUERY_CHUNK_SIZE]
        CMD = build_cmd(",".join(chunk))
        QUERY_COUNTS[tool] += 1
        try:
            result = subprocess.run(CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except Exception as e:
            log.error(f"Error while running {CMD[0]} for {len(chunk)} job IDs: {e}")
            continue
        if result.returncode != 0:
            log.error(f"{CMD[0]} failed for {len(chunk)} job IDs:\n\t{result.stderr.strip()}")
            continue
        for line in result.stdout.splitlines():
            if line:
                # sacct also prints step lines (1234.batch); keep the first, main job line
                lines_by_id.setdefault(line.split("|", 1)[0], line)
    return lines_by_id


def slurm_info(slurm_ids: Union[str, int, List[str], List[int]]) -> Union[dict, List[dict]]:
    """Get information about jobs using sacct, completed for pending/running jobs by squeue.

    Both tools are called once per QUERY_CHUNK_SIZE unique IDs.
    """
    single, slurm_ids = _as_id_list(slurm_ids)
    info = slurm_sacct_info(slurm_ids)
    active_ids = [itm["JobID"] for itm in info if itm["State"] in ("PENDING", "RUNNING")]
    if active_ids:
        squeue_by_id = {itm["JobID"]: itm for itm in slurm_squeue_info(active_ids) if itm["State"] != "UNKNOWN"}
        for itm in info:
            if itm["JobID"] in squeue_by_id:
                itm.update(squeue_by_id[itm["JobID"]])
    return info[0] if single else info

def replace_placeholders(string: str, slurm_id: str, node_list: str) -> str:
    if string is None:
        return None
    string = string.replace("%j", slurm_id)
    string = string.replace("%N", node_list)
    return string

def get_field(fields, idx, name, slurm_id):
    try:
        return fields[idx]
    except IndexError:
        log.warning(f"Missing field '{name}' for job {slurm_id}")
        return None


def _parse_sacct_line(status_line: Optional[str], slurm_id: str) -> dict:
    info = {"JobID": slurm_id}
    if status_line is None:
        info['State'] = 'UNKNOWN'
        return info
    fields = status_line.split('|')
    info['JobName'] = get_field(fields, 1, 'JobName', slurm_id)
    state = get_field(fields, 2, 'State', slurm_id)
    info['State'] = normalize_state(state) if state else 'UNKNOWN'
    info['ExitCode'] = get_field(fields, 3, 'ExitCode', slurm_id)
    info['Partition'] = get_field(fields, 4, 'Partition', slurm_id)
    info['User'] = get_field(fields, 5, 'User', slurm_id)
    info['Elapsed'] = get_field(fields, 6, 'Elapsed', slurm_id)
    info['Timelimit'] = get_field(fields, 7, 'Timelimit', slurm_id)
    info['NNodes'] = get_field(fields, 8, 'NNodes', slurm_id)
    info['NodeList'] = get_field(fields, 9, 'NodeList', slurm_id)
    info['Reason'] = get_field(fields, 10, 'Reason', slurm_id)
    info['StdOut'] = replace_placeholders(get_field(fields, 11, 'StdOut', slurm_id), slurm_id, info['NodeList'])
    info['StdErr'] = replace_placeholders(get_field(fields, 12, 'StdErr', slurm_id), slurm_id, info['NodeList'])
    return info


def parse_sacct_output(status_lines: list[str], slurm_id: str) -> dict:
    line = next((l for l in status_lines if l.split('|')[0] == str(slurm_id)), None)
    return _parse_sacct_line(line, str(slurm_id))


def slurm_sacct_info(slurm_ids: Union[str, int, List[str], List[int]]) -> Union[dict, List[dict]]:
    """Get information about jobs using sacct (one call per QUERY_CHUNK_SIZE unique IDs)."""
    single, slurm_ids = _as_id_list(slurm_ids)
    if not slurm_ids:
        return {} if single else []
    lines = _query_lines("sacct", slurm_ids, lambda ids: [
        "sacct", "-j", ids, f"--format={_SACCT_FORMAT}", "--noheader", "--parsable2"])
    all_info = [_parse_sacct_line(lines.get(sid), sid) for sid in slurm_ids]
    return all_info[0] if single else all_info


def _parse_squeue_line(status_line: Optional[str], slurm_id: str) -> dict:
    if status_line is None:
        return {"JobID": slurm_id, "State": "UNKNOWN"}
    fields = status_line.split('|')
    info = {"JobID": slurm_id}
    info['NodeList'] = get_field(fields, 1, 'NodeList', slurm_id)
    state = get_field(fields, 2, 'State', slurm_id)
    info['State'] = normalize_state(state) if state else 'UNKNOWN'
    info['Reason'] = get_field(fields, 3, 'Reason', slurm_id)
    info['Elapsed'] = get_field(fields, 4, 'Elapsed', slurm_id)
    return info


def parse_squeue_output(status_lines: list[str], slurm_id: str) -> dict:
    line = next((l for l in status_lines if l.split('|')[0] == str(slurm_id)), None)
    return _parse_squeue_line(line, str(slurm_id))


def slurm_squeue_info(slurm_ids: Union[str, int, List[str], List[int]]) -> Union[dict, List[dict]]:
    """Get information about queued jobs using squeue (one call per QUERY_CHUNK_SIZE unique IDs).

    Jobs no longer in the queue are reported as UNKNOWN.
    """
    single, slurm_ids = _as_id_list(slurm_ids)
    if not slurm_ids:
        return {} if single else []
    lines = _query_lines("squeue", slurm_ids, lambda ids: ["squeue", "-j", ids, "-h", "-o", "%i|%N|%T|%r|%M"])
    all_info = [_parse_squeue_line(lines.get(sid), sid) for sid in slurm_ids]
    return all_info[0] if single else all_info

def get_active_slurmids(dependency_entries, raise_on_fail=False):
    """ 
    Given dependency entries, return the SLURM IDs of those still active
    (ACTIVE_STATES: PENDING, RUNNING, REQUEUED, COMPLETING). Optionally raise
    an exception if any of them has failed (FAILED_STATES).
    
    Parameters
    ----------
    dependency_entries : list of dict
	    Each dict should contain at least the keys "slurm_id" and "slurm_info" (which itself is a dict containing the job's state).
    raise_on_fail : bool
        If True, raises RuntimeError if any job is in FAILED/CANCELLED state.
    
    Returns
    -------
    active_ids : list of int
        Job IDs that are still active (ACTIVE_STATES).
    """
    active_ids = []
    for entry in dependency_entries:
        slurm_id = entry["slurm_id"]
        job_name = entry.get("job_name") or entry.get("script_name") or f"node {entry.get('dag_node_name', '?')}"
        status = entry.get("slurm_info", {}).get("State", "UNKNOWN")
        if status in ACTIVE_STATES:
            active_ids.append(slurm_id)
        elif status in FAILED_STATES:
            msg = f"Dependency job {job_name}, ID={slurm_id} has failed with status {status}"
            if raise_on_fail:
                raise RuntimeError(msg)
            else:
                print("WARNING:", msg)
        else:
            if status not in ("COMPLETED"):
                print(f"WARNING: Job {job_name}, ID={slurm_id} has unexpected status {status}. Treating as finished.")
            # Else, COMPLETED or other terminal state -> ignore
    return active_ids

#Run a Slurm command and return its JobID.
def run_slurm_command(SLURM_CMD: List[str], cwd: str = None, env=None) -> Union[int, None]:
    """Submit a SLURM command; return its job ID, or None with diagnostics."""
    try:
        SLURM_CMD_STR = ' '.join(SLURM_CMD)
        result = subprocess.run(SLURM_CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd, env=env)
        if result.returncode != 0:
            log.error(f"SLURM command {SLURM_CMD_STR} failed with return code {result.returncode}.\n"
                f"  cwd: {cwd}\n  stdout: {result.stdout}\n  stderr: {result.stderr}")
            return None
        output = result.stdout.strip()
        match = re.search(r"Submitted batch job (\d+)", output)
        if match:
            return int(match.group(1))
        else:
            log.error(f"SLURM command {SLURM_CMD_STR} exited successfully, but no job ID was recognized.\n"
                "Submission outcome is uncertain; check SLURM before retrying.\n"
                f"  cwd: {cwd}\n  stdout: {result.stdout}\n  stderr: {result.stderr}")
            return None
    except Exception as e:
        log.exception("Exception while running SLURM command or parsing its output.\n aggv: %s\n cwd: %s\n env: %s", SLURM_CMD, cwd, env)
        return None


def scancel(slurm_ids: Union[str, int, List[str], List[int]]) -> Optional[str]:
    """Cancel jobs with a single scancel call.

    Returns None on success, otherwise the error message. On failure scancel
    still cancels the valid IDs (e.g. when some jobs already ended), so pass a
    single ID when the outcome per job matters.
    """
    if isinstance(slurm_ids, (str, int)):
        slurm_ids = [slurm_ids]
    slurm_ids = [str(sid) for sid in slurm_ids]
    if not slurm_ids:
        return None
    try:
        subprocess.run(["scancel", *slurm_ids], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        return (exc.stderr or "").strip() or str(exc)
    except OSError as exc:
        return str(exc)
    return None


# squeue -o fields for list_user_jobs; Reason is the pending reason or, for
# running jobs, the node list (%R).
_USER_JOB_FIELDS = ("JobID", "JobName", "State", "Partition", "Elapsed", "Timelimit", "Reason", "SubmitTime", "WorkDir")
_USER_JOB_FORMAT = "|".join(("%i", "%j", "%T", "%P", "%M", "%l", "%R", "%V", "%Z"))


def list_user_jobs(user: Optional[str] = None) -> List[dict]:
    """Return all queued/running jobs of ``user`` (default: current user) via squeue.

    Raises subprocess.CalledProcessError / OSError when squeue fails.
    """
    user = user or getpass.getuser()
    result = subprocess.run(["squeue", "-h", "-u", user, "-o", _USER_JOB_FORMAT],
                            check=True, capture_output=True, text=True)
    jobs = []
    for line in result.stdout.splitlines():
        parts = line.strip().split("|")
        if len(parts) < len(_USER_JOB_FIELDS):
            continue
        # A "|" inside the job name would shift the fields; rejoin it.
        extra = len(parts) - len(_USER_JOB_FIELDS)
        parts = [parts[0], "|".join(parts[1:2 + extra])] + parts[2 + extra:]
        info = dict(zip(_USER_JOB_FIELDS, parts))
        info["State"] = normalize_state(info["State"]) if info["State"] else "UNKNOWN"
        jobs.append(info)
    return jobs


def own_allocation_job_ids() -> set:
    """IDs of the SLURM allocation the current process runs in (empty outside SLURM)."""
    return {os.environ[var].strip() for var in ("SLURM_JOB_ID", "SLURM_JOBID") if os.environ.get(var)}


def is_own_allocation(slurm_id: Union[str, int], own_ids: Optional[set] = None) -> bool:
    """True if ``slurm_id`` (also an array task <id>_<task>) is the caller's own allocation."""
    if own_ids is None:
        own_ids = own_allocation_job_ids()
    return str(slurm_id).split("_")[0] in own_ids
