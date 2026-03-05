#!/usr/bin/env python

import os
import sys
import time
import argparse
from laupy import maxwell
from subprocess import run
from pathlib import Path
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
                ["sacct", "-j", str(job_id), "--format=State", "--noheader", "--parsable2"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            status_lines = result.stdout.strip().splitlines()
            if not status_lines:
                # Job not found; treat as finished
                continue

            # Take first line, could be multiple steps
            status = status_lines[0].split('|')[0]
            
            if status in ("PENDING", "RUNNING", "REQUEUED", "COMPLETING"):
                active_ids.append(job_id)
            elif status in ("FAILED", "CANCELLED", "TIMEOUT"):
                msg = f"Dependency job {job_id} has status {status}"
                if raise_on_fail:
                    raise RuntimeError(msg)
                else:
                    print("WARNING:", msg)
            else:
                if status not in ("COMPLETED"):
                    print(f"WARNING: Job {job_id} has unexpected status {status}")
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
    parser.add_argument("--pipeline-step", type=int, default=-1, help="Pipeline step number to create DAGs for")
    parser.add_argument("--create-pipeline-only", action="store_true", help="Only create pipeline scripts without submitting jobs")
    parser.add_argument("--delete-pipeline-only", action="store_true", help="Only create pipeline scripts without submitting jobs")
    parser.add_argument("scriptName", type=str, help="Script to execute in each working directory")

    # Parse the arguments
    ARG = parser.parse_args()
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
            print(f"Working directory {wd_rel} does not exist or is not a directory.", out=sys.stderr)
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
        print(f"SLURM script directory {SBATCH_DIR_REL} does not exist or is not a directory.", out=sys.stderr)
        sys.exit(1)

    #If --partition is not set by the user and --gpu-nodes is requested, set partition to "allgpu"
    partition_was_user_set = any(arg in ("-f", "--partition") for arg in sys.argv)
    if ARG.gpu_nodes and not partition_was_user_set:
        ARG.partition = "allgpu"
        print("GPU nodes requested without explicit -f/--partition, setting partition to 'allgpu'")


     # Check if script exists
    SCRIPTNAME = ARG.scriptName
    SCRIPT_ABS = os.path.join(SBATCH_DIR_ABS, SCRIPTNAME)
    SCRIPT_REL = os.path.join(SBATCH_DIR_REL, SCRIPTNAME)
    if not os.path.isfile(SCRIPT_ABS):
        print(f"Script {SCRIPT_REL} does not exist.", out=sys.stderr)
        sys.exit(1)

    subdirs = []
    for wd_abs, wd_rel in zip(WD_PATH_ABS, WD_PATH_REL):
        for subdir in wd_abs.iterdir():
            if subdir.is_dir() or ( subdir.is_symlink() and subdir.resolve().is_dir() ):
                if ARG.pattern is None or ARG.pattern.lower() in subdir.name.lower():
                    subdirs.append({ "subdir": subdir.name, "subdir_abs": subdir.resolve(), "subdir_rel": os.path.relpath(subdir.resolve(), ROOTDIR) })
    SLURM_ARGS_LIST = []
    # Node list handling
    node_list = []
    excluded_node_list = []
    
    if ARG.nodelist:
        node_list = maxwell.nodes_from_string(ARG.nodelist)
    elif ARG.excellent_nodes:
        node_list = maxwell.get_excellent_nodes(partition=ARG.partition)
    elif ARG.strong_nodes:
        node_list = maxwell.get_strong_nodes(partition=ARG.partition)
    elif ARG.exclude_weak_nodes:
        excluded_node_list = maxwell.get_weak_nodes(partition=ARG.partition)
    if ARG.gpu_nodes:
        gpu_nodes = maxwell.get_gpu_nodes(partition=ARG.partition)
        if len(node_list) > 0:
            node_list = list(set(node_list).intersection(set(gpu_nodes))) 
        else:
            node_list = gpu_nodes
    if len(node_list) > 0 or len(excluded_node_list) > 0:
        live_nodes = maxwell.get_live_nodes(partition=ARG.partition, idle_only=False)
        if len(node_list) > 0:
            node_list = list(set(node_list).intersection(set(live_nodes)))
            SLURM_ARGS_LIST.append(f"--nodelist={','.join(node_list)}")
        if len(excluded_node_list) > 0:
            excluded_node_list = list(set(excluded_node_list).intersection(set(live_nodes)))
            SLURM_ARGS_LIST.append(f"--exclude={','.join(excluded_node_list)}")

    if ARG.oversubscribe:
        SLURM_ARGS_LIST.append("--oversubscribe")

    SLURM_ARGS_LIST.append(f"--partition={ARG.partition}")

    for subdir_dct in subdirs:
        print(f"Preparing SLURM submission for beamtime: {subdir_dct['subdir_abs']}")
        subdir = subdir_dct["subdir"]
        SUBDIR_ABS = subdir_dct["subdir_abs"]
        SUBDIR_REL = subdir_dct["subdir_rel"]
        # Create job name
        SCRIPTNAME_NOEXT = os.path.splitext(SCRIPTNAME)[0]
        JOBNAME = f"{SCRIPTNAME_NOEXT}_{subdir}"
        if ARG.delaytime > 0:
            time.sleep(ARG.delaytime)
        SLURM_CMD = ["sbatch"] + SLURM_ARGS_LIST + [f"--job-name={JOBNAME}"]
        SLURM_CMD += ["--output=" + os.path.join(SUBDIR_REL, "pipeline", "log", f"{SCRIPTNAME_NOEXT}-%N-%j.out")]
        SLURM_CMD += ["--error=" + os.path.join(SUBDIR_REL, "pipeline", "log", f"{SCRIPTNAME_NOEXT}-%N-%j.err")]
        EXEC_CMD_REL = [SCRIPT_REL, SUBDIR_REL]
        EXEC_CMD_ABS = [SCRIPT_ABS, SUBDIR_ABS]
        if ARG.slurmargs:
            EXEC_CMD_REL.extend(shlex.split(ARG.slurmargs))
            EXEC_CMD_ABS.extend(shlex.split(ARG.slurmargs))
        SLURM_CMD_REL = SLURM_CMD.copy() + EXEC_CMD_REL
        SLURM_CMD_ABS = SLURM_CMD.copy() + EXEC_CMD_ABS
        SLURM_CMD = SLURM_CMD_REL
        print(" ".join(SLURM_CMD))
        if not ARG.dry_run:
            PIPELINE_DIR = os.path.join(SUBDIR_ABS, "pipeline")
            PIPELINE_LOG_DIR = os.path.join(PIPELINE_DIR, "log")
            if ARG.delete_pipeline_only:
                #Delete pipeline directory if requested
                removePipelineDir(PIPELINE_DIR)
            else:
                os.makedirs(PIPELINE_LOG_DIR, exist_ok=True)
                #Append command to pipeline/exec.sh
                appendCommand(EXEC_CMD_ABS, os.path.join(SUBDIR_ABS, "pipeline", "exec.sh"))
            if not ARG.create_pipeline_only and not ARG.delete_pipeline_only:
                DAG = load_dag(SUBDIR_ABS)
                if ARG.pipeline_step > -1:
                    dependency_ids = [ entry["job_id"] for entry in DAG if entry["step"] != -1 and entry["step"] < ARG.pipeline_step and "job_id" in entry ]
                    try: 
                        if len(dependency_ids) > 0:
                            active_dependencies = get_active_dependencies(dependency_ids, raise_on_fail=True)
                            if len(active_dependencies) > 0:
                                SLURM_CMD.insert(1, f"--dependency=afterok:{':'.join(str(jid) for jid in active_dependencies)}")
                        #Shall be parsing string of the type "Submitted batch job 123456"
                        result = run(SLURM_CMD, check=True, cwd=ROOTDIR, stdout=subprocess.PIPE, text=True)
                        output = result.stdout.strip()
                        # Use regex to capture the job ID (it will match "Submitted batch job 123456")
                        match = re.search(r"Submitted batch job (\d+)", output)
                        if match:
                            # Return the job ID (converted to an integer)
                            SLURMID = match.group(1)
                        else:
                            # If regex doesn't match, print message and continue
                            print(f"Could not parse SLURM submission output: {output} for {SUBDIR_REL}")
                            continue
                        DAG_ENTRY = { "step": ARG.pipeline_step, "job_id": int(SLURMID.split()[-1]) , "timestamp": time.time() , "slurm_command": " ".join(SLURM_CMD_ABS), "command": " ".join(EXEC_CMD_ABS), "dependencies": active_dependencies if len(dependency_ids) > 0 else [] }
                        DAG.append(DAG_ENTRY)
                        save_dag(SUBDIR_ABS, DAG)
                    except RuntimeError as e:
                        print(f"Not submitting job for {SUBDIR_REL} due to failed dependency: {e}")
                        continue
                    except Exception as e:
                        print(f"Error submitting job for {SUBDIR_REL}: {e}")
                        continue
                else:
                    run(SLURM_CMD)



if __name__ == "__main__":
    main()

