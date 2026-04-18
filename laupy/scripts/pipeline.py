#!/usr/bin/env python

import os
import sys
import time
import argparse
from laupy import maxwell
from laupy import slurm
from subprocess import run
from pathlib import Path
from termcolor import colored
import shlex
import json
import re

def load_dag(subdir_abs):
    dag_file = os.path.join(subdir_abs, "pipeline", "dag.json")
    if os.path.exists(dag_file):
        with open(dag_file) as f:
            return json.load(f)
    return []

def save_dag(subdir_abs, dag):
    dag_file = os.path.join(subdir_abs, "pipeline", "dag.json")
    os.makedirs(os.path.dirname(dag_file), exist_ok=True)
    with open(dag_file, "w") as f:
        json.dump(dag, f, indent=2)

def clean_dag(subdir_abs):
    save_dag(subdir_abs, [])  # Just overwrite with an empty list to clear the DAG

import subprocess

def get_active_dependencies(job_ids, raise_on_fail=False):
    """
    Given a list of SLURM job IDs, return only the IDs that are active
    (PENDING or RUNNING). Optionally raise an exception if any job has FAILED.
    
    Parameters
    ----------
    job_ids : list of int
        List of SLURM job IDs to check.
    raise_on_fail : bool
        If True, raises RuntimeError if any job is in FAILED/CANCELLED state.
    
    Returns
    -------
    active_ids : list of int
        Job IDs that are still active (PENDING or RUNNING).
    """
    active_ids = []
    
    for job_id in job_ids:
        try:
            # Query job state using sacct
            result = subprocess.run(
                ["sacct", "-j", str(job_id), "--format=JobName,State", "--noheader", "--parsable2"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            status_lines = result.stdout.strip().splitlines()
            if not status_lines:
                # Job not found; treat as finished
                continue

            # Take first line, could be multiple steps but we care about the main job state
            fields = status_lines[0].split('|')
            job_name = fields[0]
            status = fields[1]
            
            if status in ("PENDING", "RUNNING", "REQUEUED", "COMPLETING"):
                active_ids.append(job_id)
            elif status in ("FAILED", "CANCELLED", "TIMEOUT"):
                msg = f"Dependency job {job_name}, ID={job_id} has failed with status {status}"
                if raise_on_fail:
                    raise RuntimeError(msg)
                else:
                    print("WARNING:", msg)
            else:
                if status not in ("COMPLETED"):
                    print(f"WARNING: Job {job_name}, ID={job_id} has unexpected status {status}. Treating as finished.")
            # Else, COMPLETED or other terminal state -> ignore
        except Exception as e:
            print(f"Error checking SLURM job {job_id}: {e}")
            raise
    return active_ids


def appendCommand(cmd_list, pipeline_file):
    """
    Append a command to the pipeline script file.
    
    Parameters:
    - cmd_list: list of command arguments (like ['python', 'script.py', 'arg'])
    - pipeline_file: path to pipeline script (str)
    """
    pipeline_dir = os.path.dirname(pipeline_file)
    os.makedirs(pipeline_dir, exist_ok=True)

    # Determine if we need to write the shebang
    write_shebang = not os.path.exists(pipeline_file)

    with open(pipeline_file, "a") as f:
        if write_shebang:
            f.write("#!/bin/bash\n# Auto-generated pipeline script\n\n")
        # Convert command list to safely escaped string
        cmd_str = " ".join(shlex.quote(str(arg)) for arg in cmd_list)
        f.write(cmd_str + "\n")


def removePipelineDir(pipeline_dir):
    """
    Safely delete a pipeline directory and its log subdirectory.
    
    Rules:
    - Delete all files directly in pipeline_dir
    - Delete all files in pipeline_dir/log
    - Remove log directory
    - Remove pipeline_dir itself
    - Any other subdirectories in pipeline_dir remain untouched
    """
    if not os.path.exists(pipeline_dir):
        print(f"No pipeline directory to delete at {pipeline_dir}")
        return

    if not os.path.isdir(pipeline_dir):
        print(f"{pipeline_dir} exists but is not a directory! Skipping deletion.")
        return

    log_dir = os.path.join(pipeline_dir, "log")

    print(f"Deleting contents of pipeline directory safely: {pipeline_dir}")

    # Delete files in pipeline_dir
    for entry in os.listdir(pipeline_dir):
        entry_path = os.path.join(pipeline_dir, entry)
        if os.path.isfile(entry_path):
            os.remove(entry_path)

    # Delete log directory and its files
    if os.path.exists(log_dir) and os.path.isdir(log_dir):
        for entry in os.listdir(log_dir):
            file_path = os.path.join(log_dir, entry)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(log_dir)  # remove log directory itself

    # Finally remove pipeline_dir (should be empty except for unexpected dirs)
    try:
        os.rmdir(pipeline_dir)
    except OSError:
        print(f"Pipeline directory {pipeline_dir} not empty (other subdirs preserved).")

def parse_comma_separated(value):
    """
    Parse a comma-separated string into a list of strings.
    
    Example:
    "dir1,dir2,dir3" -> ["dir1", "dir2", "dir3"]
    """
    return [item.strip() for item in value.split(",") if item.strip()]

def main():
    parser = argparse.ArgumentParser(description="Submit jobs to SLURM.")
    
    # Define command-line arguments
    parser.add_argument("-d", "--root-dir", type=str, help="Root directory (defaults to current directory)", default=None)
    parser.add_argument("-w", "--working-dir", type=parse_comma_separated, action="append", required=False, help="One or more working directories. Can be comma-separated or repeated. Defaults to wd.", default=None)
    parser.add_argument("--slurm-dir", type=str, help="Working directory, shall be subdirectory of the root dir (defaults to 'wd')", default="sbatch")
    parser.add_argument("-p", "--pattern", type=str, help="Pattern for directory matching")
    parser.add_argument("-a", "--slurmargs", type=str, help="Additional SLURM arguments")
    parser.add_argument("-f", "--partition", type=str, help="Partition to use (defaults to 'allcpu,allgpu')", default="allcpu,allgpu")
    parser.add_argument("-n", "--nodelist", type=str, help="Specify node list")
    parser.add_argument("-o", "--oversubscribe", action="store_true", help="Allow oversubscription")
    parser.add_argument("-t", "--delaytime", type=int, help="Delay time in seconds before the next submit", default=0)
    parser.add_argument('-g', '--gpu-nodes', action='store_true', help="Select only nodes with GPUs")
    parser.add_argument("-e", "--excellent-nodes", action="store_true", help="Use excellent nodes only")
    parser.add_argument("-s", "--strong-nodes", action="store_true", help="Use strong nodes only")
    parser.add_argument("-x", "--exclude-weak-nodes", action="store_true", help="Exclude weak nodes")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the submission without actually submitting jobs or crating files")
    parser.add_argument("--verbose", action="store_true", help="Print detailed information about the submission process")
    parser.add_argument("--pipeline-step", type=int, default=-1, help="Pipeline step number to create DAGs for")
    # ---- Pipeline management: mutually exclusive subgroup ----
    pipeline_group = parser.add_mutually_exclusive_group()
    pipeline_group.add_argument("--status", action="store_true", help="Show status of existing pipeline/DAG and exit.")
    pipeline_group.add_argument("--create", action="store_true", help="Create pipeline scripts/DAG and exit.")
    pipeline_group.add_argument("--delete", action="store_true", help="Delete existing pipeline/DAG artifacts and exit.")
    pipeline_group.add_argument("--clean-dag", action="store_true", help="Clean DAG entries for the specified pipeline.")

    # Parse the arguments
    ARG = parser.parse_args()
    if ARG.verbose:
        print("Parsed arguments:")
        for arg_name, arg_value in vars(ARG).items():
            print(f"  {arg_name}: {arg_value}")

    if ARG.root_dir is None:
        ROOTDIR = Path.cwd()
    else:
        ROOTDIR = Path(ARG.root_dir)
    ROOTDIR = ROOTDIR.expanduser().resolve()
    print(f"Changing to root directory: {ROOTDIR}")
    os.chdir(ROOTDIR)
    # Normalize working directory path
    working_dirs = ["wd"] if ARG.working_dir is None else [ d for group in ARG.working_dir for d in group ]
    WD = [Path(d) for d in working_dirs]
    WD_PATH_ABS = [ d.resolve() if d.is_absolute() else (Path(ROOTDIR) / d).resolve() for d in WD ]
    WD_PATH_REL = [ os.path.relpath(abs_path, ROOTDIR) for abs_path in WD_PATH_ABS ]
    for wd, wd_abs, wd_rel in zip(WD, WD_PATH_ABS, WD_PATH_REL):
        if not wd.is_dir() or ( wd.is_symlink() and not wd.resolve().is_dir() ):
            print(f"Working directory {wd_rel} does not exist or is not a directory.", file=sys.stderr)
            sys.exit(1)
    # Normalize SLURM script directory path
    SBATCH_DIR = Path(ARG.slurm_dir)
    if SBATCH_DIR.is_absolute():
        SBATCH_DIR_ABS = SBATCH_DIR
    else:
        SBATCH_DIR_ABS = (Path(ROOTDIR) / SBATCH_DIR)
    SBATCH_DIR_REL = os.path.relpath(SBATCH_DIR_ABS, ROOTDIR)
    SBATCH_DIR_ABS = SBATCH_DIR_ABS.resolve()
    if not SBATCH_DIR.is_dir() or SBATCH_DIR.is_symlink() and not SBATCH_DIR.resolve().is_dir():
        print(f"SLURM script directory {SBATCH_DIR_REL} does not exist or is not a directory.", file=sys.stderr)
        sys.exit(1)

    subdirs = []
    for wd_abs, wd_rel in zip(WD_PATH_ABS, WD_PATH_REL):
        for subdir in wd_abs.iterdir():
            if subdir.is_dir() or ( subdir.is_symlink() and subdir.resolve().is_dir() ):
                if ARG.pattern is None or ARG.pattern.lower() in subdir.name.lower():
                    subdirs.append({ "subdir": subdir.name, "subdir_abs": str(subdir.resolve()), "subdir_rel": str(os.path.relpath(subdir.resolve(), ROOTDIR)) })

    if len(subdirs) == 0:
        if ARG.pattern is not None:
            print("No subdirectories of %s match the specified pattern '%s'." % (", ".join(WD_PATH_REL), ARG.pattern), file=sys.stderr)
        else:
            print("No subdirectories of %s found." % (", ".join(WD_PATH_REL)), file=sys.stderr)
        sys.exit(1)
    SLURM_ARGS_LIST = []
    # Node list handling
    node_list = []
    excluded_node_list = []
    
    for subdir_dct in subdirs:
        if ARG.verbose:
            print(f"Processing subdir: {subdir_dct['subdir_rel']} (abs: {subdir_dct['subdir_abs']})")
        subdir = subdir_dct["subdir"]
        SUBDIR_ABS = subdir_dct["subdir_abs"]
        SUBDIR_REL = subdir_dct["subdir_rel"]
        PIPELINE_DIR = os.path.join(SUBDIR_ABS, "pipeline")
        PIPELINE_LOG_DIR = os.path.join(PIPELINE_DIR, "log")
        if ARG.status:
            DAG = load_dag(SUBDIR_ABS)
            if len(DAG) == 0:
                print(f"No pipeline/DAG found for {SUBDIR_REL}.")
            else:
                output_lines = [f"{SUBDIR_REL} pipeline status:"]
                for entry in DAG:
                    step = entry.get("step", "N/A")
                    job_id = entry.get("job_id", "N/A")
                    entry_info = slurm.slurm_info(job_id) if job_id != "N/A" else {"State": "UNKNOWN"}
                    job_state = entry_info["State"]
                    job_name = entry_info.get("JobName", "N/A")
                    slurm_command = entry.get("slurm_command", "")
                    command = entry.get("command", "")
                    dependencies = entry.get("dependencies", [])
                    basic_info = f"\tStep: {step}, Job ID: {job_id}, Job Name: {job_name}, State: {job_state}"
                    retired = entry.get("retired", False)
                    if retired == True:
                        continue  # Skip retired jobs
                    if job_state in ("PENDING"):
                        reason = entry_info.get("Reason", "N/A")
                        if reason in ("N/A", "None", ""):
                            output_lines.append(basic_info)
                        else:
                            output_lines.append(colored(f"{basic_info}, Reason: {reason}", "yellow"))
                    elif job_state in ("RUNNING"):
                         elapsed = entry_info.get("Elapsed", "N/A")
                         time_limit = entry_info.get("Timelimit", "N/A")
                         output_lines.append(colored(f"{basic_info}, Time limit: {time_limit}, Elapsed: {elapsed}", "green"))
                    elif job_state in ("REQUEUED", "COMPLETING"):
                        output_lines.append(f"\tStep: {step}, Job ID: {job_id}, State: {job_state}, Job Name: {job_name}")
                    elif job_state in ("FAILED", "CANCELLED", "TIMEOUT"):
                        output_lines.append(colored(f"\tStep: {step}, Job ID: {job_id}, State: {job_state}, Job Name: {job_name}", "red"))
                    elif job_state not in ("COMPLETED"):
                        output_lines.append(colored(f"\tStep: {step}, Job ID: {job_id}, State: {job_state}, Job Name: {job_name} (unexpected state)", "yellow"))
                if len(output_lines) > 1:
                    print("\n".join(output_lines))
        if ARG.create:
            os.makedirs(PIPELINE_LOG_DIR, exist_ok=True)
        if ARG.clean_dag:
            #Clean DAG entries for the specified pipeline to resolve failed dependencies without creating new pipeline scripts or submitting jobs
            print(f"In {subdir_dct['subdir_abs']}, cleaning DAG file pipeline/dag.json")
            clean_dag(SUBDIR_ABS)
        if ARG.delete:
            #Delete pipeline directory if requested
            print(f"In {subdir_dct['subdir_abs']}, deleting pipeline directory {PIPELINE_DIR} and all its contents (logs, DAGs, scripts)")
            removePipelineDir(PIPELINE_DIR)
            #Append command to pipeline/exec.sh



if __name__ == "__main__":
    main()

