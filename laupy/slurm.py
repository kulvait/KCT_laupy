#!/usr/bin/env python

# Utility functions
from typing import Union, List, Dict
import subprocess
import logging
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


def normalize_state(state: str) -> str:
    return state.split()[0].replace("+", "")


def slurm_info(slurm_ids: Union[str, int, List[str], List[int]]) -> Union[dict, List[dict]]:
    """Get information about a job using sacct, and if that fails, use squeue."""
    non_list_input = isinstance(slurm_ids, str) or isinstance(slurm_ids, int)
    if non_list_input:
        slurm_ids = [str(slurm_ids)]
    else:
        slurm_ids = [str(sid) for sid in slurm_ids]
    info = slurm_sacct_info(slurm_ids)
    squeue_info_slurmids = [
        itm['JobID'] for itm in info if itm['State'] == "PENDING" or itm['State'] == "RUNNING"]
    if len(squeue_info_slurmids) > 0:
        squeue_info_output = slurm_squeue_info(squeue_info_slurmids)
    else:
        squeue_info_output = []
    for squeue_info in squeue_info_output:
        for i in info:
            if i['JobID'] == squeue_info['JobID']:
                i.update(squeue_info)
    if non_list_input:
        return info[0]
    else:
        return info

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


def parse_sacct_output(status_lines: list[str], slurm_id: str) -> dict:
    info = {"JobID": slurm_id}
    status_line = None
    for line in status_lines:
        if line.split('|')[0] == str(slurm_id):
            status_line = line
            break
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


def slurm_sacct_info_fallback_single(slurm_ids, return_unknown=False):
    if return_unknown:
        return {"JobID": slurm_ids[0], "State": "UNKNOWN"}
    else:
        return [slurm_sacct_info(slurm_id) for slurm_id in slurm_ids]


def slurm_sacct_info(slurm_ids: Union[str, int, List[str], List[int]]) -> Union[dict, List[dict]]:
    """Get information about a job using sacct."""
    non_list_input = isinstance(slurm_ids, str) or isinstance(slurm_ids, int)
    if non_list_input:
        slurm_ids = [str(slurm_ids)]
    else:
        slurm_ids = [str(sid) for sid in slurm_ids]
    all_info = []
    if len(slurm_ids) == 0:
        log.warning("No job IDs provided to slurm_sacct_info.")
        if non_list_input:
            return {}
        else:
            return []
    jobs_to_query = ",".join(slurm_ids)
    CMD = ['sacct', '-j', jobs_to_query,
           '--format=JobID,JobName,State,ExitCode,Partition,User,Elapsed,Timelimit,NNodes,NodeList,Reason,StdOut,StdErr',
           '--noheader', '--parsable2']
    try:
        result = subprocess.run(
            CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            log.error(
                f"Error while running %s:\n\t{result.stderr}" % (" ".join(CMD)))
            return slurm_sacct_info_fallback_single(slurm_ids, non_list_input)
        status_lines = [l for l in result.stdout.strip().splitlines() if l]
        if not status_lines:
            log.error(f"No output from %s for job IDs %s" %
                      (" ".join(CMD), jobs_to_query))
            return slurm_sacct_info_fallback_single(slurm_ids, non_list_input)
        # Find the main job line (the one with the job ID without dot and step so jobid instead of jobid.0 or jobid.batch)
        for slurm_id in slurm_ids:
            info = parse_sacct_output(status_lines, slurm_id)
            all_info.append(info)
    except Exception as e:
        log.error(f"Error while running %s:\n\t{e}" % (" ".join(CMD)))
        all_info = []
        for slurm_id in slurm_ids:
            info = {"JobID": slurm_id, "State": "UNKNOWN"}
            all_info.append(info)
    if non_list_input:
        return all_info[0]
    else:
        return all_info


def parse_squeue_output(status_lines: list[str], slurm_id: str) -> dict:
    status_line = None
    for line in status_lines:
        if line.split('|')[0] == str(slurm_id):
            status_line = line
            break
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


def slurm_squeue_info_fallback_single(slurm_ids, return_unknown=False):
    if return_unknown:
        return {"JobID": slurm_ids[0], "State": "UNKNOWN"}
    else:
        return [slurm_squeue_info(slurm_id) for slurm_id in slurm_ids]


def slurm_squeue_info(slurm_ids: Union[str, int, List[str], List[int]]) -> Union[dict, List[dict]]:
    """Get information about a job using squeue."""
    non_list_input = isinstance(slurm_ids, str) or isinstance(slurm_ids, int)
    if non_list_input:
        slurm_ids = [str(slurm_ids)]
    else:
        slurm_ids = [str(sid) for sid in slurm_ids]
    all_info = []
    jobs_to_query = ",".join(slurm_ids)
    CMD = ['squeue', '-j', jobs_to_query, '-h', '-o', "%i|%N|%T|%r|%M"]
    try:
        result = subprocess.run(
            CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            log.error(
                f"Error while running %s:\n\t{result.stderr}" % (" ".join(CMD)))
            return slurm_squeue_info_fallback_single(slurm_ids, non_list_input)
        status_lines = [l for l in result.stdout.strip().splitlines() if l]
        if not status_lines:
            log.error(f"No output from %s for job IDs %s" %
                      (" ".join(CMD), jobs_to_query))
            return slurm_squeue_info_fallback_single(slurm_ids, non_list_input)
        for slurm_id in slurm_ids:
            info = parse_squeue_output(status_lines, slurm_id)
            all_info.append(info)
    except Exception as e:
        log.error(f"Error while running %s:\n\t{e}" % (" ".join(CMD)))
        all_info = []
        for slurm_id in slurm_ids:
            info = {"JobID": slurm_id, "State": "UNKNOWN"}
            all_info.append(info)
    if non_list_input:
        return all_info[0]
    else:
        return all_info
