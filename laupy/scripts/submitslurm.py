#!/usr/bin/env python

import os
import sys
import time
import argparse
from laupy import maxwell
from subprocess import run
import shlex
import logging
from laupy.cli import add_common_arguments, parse_comma_separated, print_arguments, resolve_layout
from laupy.entries import EntriesStore, select_entries, retire_entries, cancel_entries
from laupy.flow import clean_entries, remove_pipeline_dir, schedule_slurm_job

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

STATE_CHOICES = ["NONE", "PENDING", "RUNNING", "REQUEUED", "COMPLETING", "FAILED", "CANCELLED", "TIMEOUT", "COMPLETED"]


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


def range_entries(range_name, node_entries, cmd_entries, jobname_entries):
    """Entries of the submitted node covered by a --skip/--cancel/--retire-range value."""
    return {"cmd": cmd_entries, "step": node_entries, "jobname": jobname_entries}.get(range_name, [])


def main():
    parser = argparse.ArgumentParser(description="Submit jobs to SLURM using laupy")

    # Define command-line arguments
    add_common_arguments(parser, dry_run_help="Simulate the submission without actually submitting jobs or creating files")
    parser.add_argument("-a", "--slurmargs", type=str, help="Additional SLURM arguments")
    parser.add_argument("-f", "--partition", type=str, help="Partition to use (defaults to 'allcpu,allgpu')", default="allcpu,allgpu")
    parser.add_argument("-n", "--nodelist", type=str, help="Specify node list")
    parser.add_argument("-o", "--oversubscribe", action="store_true", help="Allow oversubscription")
    parser.add_argument("-t", "--delaytime", type=int, help="Delay time in seconds before the next submit", default=0)
    parser.add_argument('-g', '--gpu-nodes', action='store_true', help="Select only nodes with GPUs")
    parser.add_argument("-e", "--excellent-nodes", action="store_true", help="Use excellent nodes only")
    parser.add_argument("-s", "--strong-nodes", action="store_true", help="Use strong nodes only")
    parser.add_argument("-x", "--exclude-weak-nodes", action="store_true", help="Exclude weak nodes")
    scheduling_group = parser.add_mutually_exclusive_group()
    scheduling_group.add_argument("--pipeline-step", type=str, default="-1", help="Pipeline step number or DAG node name. Uses only explicit DAG dependencies. Default is -1, which are not put or evaluated against DAG.")
    parser.add_argument("--dependencies", type=parse_comma_separated, action="append", default=None, metavar="NODE[,NODE...]", help="Dependencies for non numeric --pipeline-step/DAG node, comma-separated or repeated. Can also be numeric pipeline steps (e.g., 0,1,2), numeric pipeline steps can not have explicit --dependencies. Default is None.")
    parser.add_argument("--skip-range", type=str, choices=["none", "cmd", "jobname", "step"], default="cmd", help="Scope of entries of the same node that make the new submission be skipped: 'none' (never skip), 'cmd' (same command), 'jobname' (same SLURM job name), 'step' (any entry of the node). Default is 'cmd'.")
    parser.add_argument("--skip-state", nargs="+", choices=STATE_CHOICES, default=["PENDING", "RUNNING", "REQUEUED"], help="SLURM job states to yield to command submitted in the same step with coresponging state. Default is PENDING, RUNNING, REQUEUED.")
    parser.add_argument("--retire-range", type=str, choices=["none", "cmd", "step"], default="cmd", help="Scope of retiring entries on new submission: 'none' (retire nothing), 'cmd' (retire only entries with the same command and step), 'step' (retire all entries with the same step regardless of command). Default is 'cmd'.")
    parser.add_argument("--retire-state", nargs="+", choices=STATE_CHOICES, default=["FAILED", "CANCELLED", "TIMEOUT"], help="SLURM job states that trigger retiring of entries in the same step on new submission with --retire-range set to 'cmd' or 'step'. Default is FAILED, CANCELLED, TIMEOUT.")
    parser.add_argument("--cancel-range", type=str, choices=["none", "cmd", "step"], default="none", help="Scope of cancelling active jobs on new submission: 'none' (cancel nothing), 'cmd' (cancel only jobs with the same command and step), 'step' (cancel all jobs with the same step regardless of command). Default is 'none'.")
    parser.add_argument("--cancel-state", nargs="+", choices=STATE_CHOICES, default=["NONE"], help="SLURM job states that trigger cancellation of active jobs in the same step on new submission with --cancel-range set to 'cmd' or 'step'. Default is NONE (no cancellations).")
    # ---- Pipeline directory management (same as `pipeline create|clean|delete`) ----
    pipeline_group = parser.add_mutually_exclusive_group()
    pipeline_group.add_argument("--create-pipeline-only", action="store_true", help="Create only the pipeline directory and exit (scriptName not required); same as `pipeline create`.")
    pipeline_group.add_argument("--clean-dag", action="store_true", help="Forget the pipeline entries and exit (scriptName not required); same as `pipeline clean`.")
    pipeline_group.add_argument("--delete-pipeline-only", action="store_true", help="Delete the pipeline directory and exit (scriptName not required); same as `pipeline delete`.")
    parser.add_argument("scriptName", type=str, nargs="?", help="Script to execute in each working directory")

    # Parse the arguments
    ARG = parser.parse_args()
    if ARG.verbose:
        print_arguments(ARG)

    pipeline_mode = ARG.create_pipeline_only or ARG.clean_dag or ARG.delete_pipeline_only
    if not pipeline_mode and ARG.scriptName is None:
        parser.error("scriptName is required unless using --create-pipeline-only, --clean-dag, or --delete-pipeline-only.")
    layout = resolve_layout(ARG)

    #If --partition is not set by the user and --gpu-nodes is requested, set partition to "allgpu"
    partition_was_user_set = any(arg in ("-f", "--partition") for arg in sys.argv)
    if ARG.gpu_nodes and not partition_was_user_set:
        ARG.partition = "allgpu"
        print("GPU nodes requested without explicit -f/--partition, setting partition to 'allgpu'")

    # Check if script exists
    if not pipeline_mode:
        SCRIPTNAME = ARG.scriptName
        SCRIPT_ABS = os.path.join(layout.sbatch_abs, SCRIPTNAME)
        SCRIPT_REL = os.path.join(layout.sbatch_rel, SCRIPTNAME)
        if not os.path.isfile(SCRIPT_ABS):
            print(f"Script {SCRIPT_REL} does not exist.", file=sys.stderr)
            sys.exit(1)

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

    if ARG.verbose:
        if len(node_list) == 0:
            print("No valid nodes found based on the specified criteria. Submitting without node restrictions.")
        else:
            print(f"Submitting to nodes: {','.join(node_list)}")

    if ARG.oversubscribe:
        SLURM_ARGS_LIST.append("--oversubscribe")

    SLURM_ARGS_LIST.append(f"--partition={ARG.partition}")

    # DAG node of the submission (resolved once for all execution units)
    dependencies = [dep for group in ARG.dependencies for dep in group] if ARG.dependencies else []
    dag_node_name = None
    if not pipeline_mode and ARG.pipeline_step != "-1":
        try:
            integer_pipeline_step = int(ARG.pipeline_step)
            if integer_pipeline_step < 0:
                print(f"Negative numeric pipeline step {integer_pipeline_step} different from -1 is not allowed.", file=sys.stderr)
                sys.exit(1)
            dag_node_name = f"{integer_pipeline_step}"
            if ARG.dependencies is not None:
                log.error(f"Error: --dependencies can not be used with numeric --pipeline-step {ARG.pipeline_step}. Use a non-numeric DAG node name for --pipeline-step to specify explicit dependencies.")
        except ValueError:
            dag_node_name = ARG.pipeline_step

    # ENTRIES of all execution units with one batched SLURM query for the
    # submitted node and its ancestors (dependencies, skip/retire/cancel checks).
    store = None
    if dag_node_name is not None and not ARG.dry_run:
        store = EntriesStore([processing_unit_subdir.abs for processing_unit_subdir in layout.units])
        store.refresh(limit_to_ancestors_including_node=dag_node_name, update_retired=False, update_negative_step=False, filter_terminal_states=True, node_dependencies=dependencies)
        if ARG.verbose:
            from laupy.slurm import QUERY_COUNTS
            print(f"SLURM queries: {QUERY_COUNTS}")

    for processing_unit_subdir in layout.units:
        if ARG.verbose:
            print(f"Processing {processing_unit_subdir.rel} (abs: {processing_unit_subdir.abs})")
        subdir = processing_unit_subdir.name
        SUBDIR_ABS = processing_unit_subdir.abs
        SUBDIR_REL = processing_unit_subdir.rel
        # Create job name
        if not pipeline_mode:
            SCRIPTNAME_NOEXT = os.path.splitext(SCRIPTNAME)[0]
            JOBNAME = f"{SCRIPTNAME_NOEXT}_{subdir}"
            EXPORTS = {"PIPELINE_SCRIPTNAME": SCRIPTNAME, "PIPELINE_JOBNAME": JOBNAME, "LD_PRELOAD": ""}
            EXPORT_STR = ",".join(f"{key}={value}" for key, value in EXPORTS.items())
            if ARG.delaytime > 0:
                time.sleep(ARG.delaytime)
            SLURM_CMD = ["sbatch"] + SLURM_ARGS_LIST
            SLURM_CMD += [f"--job-name={JOBNAME}"]
            SLURM_CMD += [f"--export=ALL,{EXPORT_STR}"]
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
            print(f"SLURM in {SUBDIR_ABS}")
            print(" ".join(SLURM_CMD))
        if ARG.dry_run:
            continue
        PIPELINE_DIR = os.path.join(SUBDIR_ABS, "pipeline")
        PIPELINE_LOG_DIR = os.path.join(PIPELINE_DIR, "log")
        if ARG.delete_pipeline_only:
            print(f"In {SUBDIR_ABS}, deleting pipeline directory {PIPELINE_DIR} and all its contents (logs, entries, scripts)")
            remove_pipeline_dir(PIPELINE_DIR)
            continue
        if ARG.clean_dag:
            # Forget the entries to resolve failed dependencies without creating new pipeline scripts or submitting jobs
            print(f"In {SUBDIR_ABS}, cleaning entries file pipeline/dag.json")
            clean_entries(SUBDIR_ABS)
            continue
        os.makedirs(PIPELINE_LOG_DIR, exist_ok=True)
        if ARG.create_pipeline_only:
            continue
        if dag_node_name is None:
            run(SLURM_CMD)  # --pipeline-step -1: plain submission outside the DAG
            continue

        ENTRIES = store[SUBDIR_ABS]
        # Non-retired entries of the submitted node, by scope, to evaluate skip, cancel and retire
        node_entries = [entry for entry in ENTRIES if entry.get("dag_node_name") == dag_node_name and "job_id" in entry and not entry.get("retired", False)]
        cmd_entries = [entry for entry in node_entries if entry.get("command") == " ".join(EXEC_CMD_ABS)]
        jobname_entries = [entry for entry in node_entries if entry.get("slurm_info", {}).get("JobName", "") == JOBNAME]
        # Skip if an entry in scope is still in one of --skip-state
        skip_entries = select_entries(range_entries(ARG.skip_range, node_entries, cmd_entries, jobname_entries), ARG.skip_state)
        if skip_entries:
            skip_ids = [entry.get("slurm_id") or entry["job_id"] for entry in skip_entries]
            print(f"Skipping submission for {SUBDIR_REL} because there are {len(skip_entries)} active entries with the same skip range {ARG.skip_range} in {ARG.skip_state}: {skip_ids}")
            continue
        try:
            # Cancel active jobs in --cancel-range/--cancel-state before submitting the replacement
            cancel_candidates = select_entries(range_entries(ARG.cancel_range, node_entries, cmd_entries, jobname_entries), ARG.cancel_state)
            cancelled = []
            for result in cancel_entries(ENTRIES, [entry["job_id"] for entry in cancel_candidates], refresh=False):
                if result["error"] is None:
                    print(f"Cancelled active job with ID {result['slurm_id']} due to new submission with --cancel-range {ARG.cancel_range} and --cancel-state {ARG.cancel_state}")
                    cancelled.append(result["entry"]["job_id"])
                else:
                    print(f"Could not cancel job with ID {result['slurm_id']}: {result['error']}")
            retire_candidates = select_entries(range_entries(ARG.retire_range, node_entries, cmd_entries, jobname_entries), ARG.retire_state)
            NEW_ENTRY = schedule_slurm_job(ENTRIES, dag_node_name, SCRIPTNAME, JOBNAME, SUBDIR_ABS, SLURM_CMD_ABS, EXEC_CMD_ABS, node_dependencies=dependencies)
            if NEW_ENTRY is not None:
                ENTRIES.append(NEW_ENTRY)
                # The replaced entries are retired and point to the entry that replaced them
                retire_entries(ENTRIES, cancelled, retired_by=NEW_ENTRY["job_id"],
                               reason=f"Cancelled due to new submission with --cancel-range {ARG.cancel_range} and --cancel-state {ARG.cancel_state}")
                for entry in retire_entries(ENTRIES, [entry["job_id"] for entry in retire_candidates], retired_by=NEW_ENTRY["job_id"],
                                            reason=f"Retired due to new submission with --retire-range {ARG.retire_range} and --retire-state {ARG.retire_state}"):
                    print(f"Retiring entry with job ID {entry['job_id']} due to new submission with --retire-range {ARG.retire_range} and --retire-state {ARG.retire_state}")
                appendCommand(EXEC_CMD_ABS, os.path.join(SUBDIR_ABS, "pipeline", "exec.sh"))
        except RuntimeError as e:
            print(f"Not submitting job for {SUBDIR_REL} due to failed dependency: {e}")
        except Exception as e:
            print(f"Error submitting job for {SUBDIR_REL}: {e}")
        finally:
            # Persist each subdir right away: new entry, retirements, cancellations
            store.save([SUBDIR_ABS])


if __name__ == "__main__":
    main()
