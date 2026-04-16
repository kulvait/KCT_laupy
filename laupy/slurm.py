#!/usr/bin/env python

# Utility functions

import subprocess


def normalize_state(state: str) -> str:
    return state.split()[0].replace("+", "")

def job_info(job_id):
    """Get information about a job using sacct."""
    info = {}
    info['JobID'] = job_id 
    try:
        result = subprocess.run(['sacct', '-j', str(job_id), '--format=JobID,JobName,State,ExitCode,Partition,User,Elapsed,Timelimit,NNodes,NodeList,Reason', '--noheader', '--parsable2'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            info['State'] = 'UNKNOWN'
            return info
        status_lines = lines = [l for l in result.stdout.strip().splitlines() if l]
        if not status_lines:
            info['State'] = 'UNKNOWN'
            return info
        #Find the main job line (the one with the job ID without dot and step so jobid instead of jobid.0 or jobid.batch)
        status_line = None
        for line in status_lines:
            if line.split('|')[0] == str(job_id):
                status_line = line
                break
        if status_line is None:
            info['State'] = 'UNKNOWN'
            return info
        fields = status_line.split('|')
        info['JobName'] = fields[1]
        info['State'] = normalize_state(fields[2])
        info['ExitCode'] = fields[3]
        info['Partition'] = fields[4]
        info['User'] = fields[5]
        info['Elapsed'] = fields[6]
        info['Timelimit'] = fields[7]
        info['NNodes'] = fields[8]
        info['NodeList'] = fields[9]
        if info['State'] == 'PENDING':
            info_squeue = squeue_info(job_id)
            info['Reason'] = info_squeue.get('Reason', 'UNKNOWN')
        else:
            info['Reason'] = fields[10]
        return info
    except Exception as e:
        info['State'] = 'UNKNOWN'
        return info

def squeue_info(job_id):
    """Get information about a job using squeue."""
    info = {}
    info['JobID'] = job_id 
    try:
        result = subprocess.run(['squeue', '-j', str(job_id), '-h', '-o', "%T|%r|%M"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0 or not result.stdout.strip():
            info['State'] = 'UNKNOWN'
            info['Reason'] = 'UNKNOWN'
            info['Elapsed'] = 'UNKNOWN'
            return info
        info['State'], info['Reason'], info['Elapsed'] = result.stdout.strip().split('|')
        info['State'] = normalize_state(info['State'])
        return info
    except Exception as e:
        info['State'] = 'UNKNOWN'
        info['Reason'] = 'UNKNOWN'
        info['Elapsed'] = 'UNKNOWN'
        return info
