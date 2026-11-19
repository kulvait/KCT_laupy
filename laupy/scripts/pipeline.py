#!/usr/bin/env python

import os
import sys
import argparse
import re
from termcolor import colored
from laupy.cli import add_common_arguments, print_arguments, resolve_directory_tree
from laupy.pipeline_entries import (RETIRABLE_STATES, CANCELLABLE_STATES, PipelineEntriesStore, entry_slurm_id,
                           select_pipeline_entries, retire_pipeline_entries, cancel_pipeline_entries)
from laupy.flow import load_pipeline_entries, clean_pipeline_entries, remove_pipeline_dir

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


def job_basic_info(entry, state=None):
    """One-line description of an entry used by retire/cancel output."""
    slurm_info = entry.get("slurm_info", {})
    return (f"\tNode: {entry.get('dag_node_name')}, Job ID: {entry.get('job_id', 'N/A')}, "
            f"Slurm ID: {entry_slurm_id(entry)}, Job Name: {slurm_info.get('JobName', 'N/A')}, "
            f"State: {state or slurm_info.get('State', 'UNKNOWN')}")


_ENTRY_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def parse_job_id(value):
    """argparse type for --job: a SLURM job ID (integer, e.g. 24170104) or an entry job_id (32 hex characters)."""
    value = value.strip()
    if value.isdigit() or _ENTRY_ID_RE.match(value):
        return value
    raise argparse.ArgumentTypeError(f"expected a SLURM job ID (e.g. 24170104) or an entry job_id (32 hex characters), got {value!r}")


# Subcommands working on the entries; their PIPELINE_ENTRIES are loaded for all
# processing units at once and refreshed from SLURM with one batched query.
ENTRY_SUBCOMMANDS = ("status", "retire", "cancel", "log")


def main():
    parser = argparse.ArgumentParser(
        description="laupy pipeline management tool: status and lifecycle of the scheduled SLURM jobs (entries) of pipelines.",
        epilog="Common options (-d, -w, --slurm-dir, -p, --dry-run, --verbose) may be given before or after the subcommand.",
    )
    add_common_arguments(parser, dry_run_help="Only print what create/clean/delete/retire/cancel would do, change nothing")
    common = argparse.ArgumentParser(add_help=False)
    add_common_arguments(common, suppress_defaults=True, dry_run_help="Only print what create/clean/delete/retire/cancel would do, change nothing")
    subparsers = parser.add_subparsers(dest="subcommand", title="subcommands", metavar="{create,clean,delete,status,log,retire,cancel}", required=True)
    # pipeline directory management
    subparsers.add_parser("create", parents=[common], help="Create the pipeline directory structure (pipeline/log) in each processing unit; submission also does this implicitly")
    subparsers.add_parser("clean", parents=[common], help="Forget all entries in pipeline/dag.json (job history is lost, log files are kept)")
    subparsers.add_parser("delete", parents=[common], help="Delete pipeline/ with its entries and logs (job history is lost; other subdirectories are kept)")
    # log action for showing SLURM logs for a specific pipeline step or job ID
    log_parser = subparsers.add_parser("log", parents=[common], help="Show SLURM logs for the specified job")
    log_parser.add_argument("id", type=str, help="Entry job_id or SLURM job ID to show logs for")
    log_parser.add_argument("--stdout", action="store_true", help="Show the standard output logs for the specified pipeline step.")
    log_parser.add_argument("--stderr", action="store_true", help="Show the error output logs for the specified pipeline step.")
    # status action for showing status of all pipeline steps with color coding
    status_parser = subparsers.add_parser("status", parents=[common], help="Show status of all pipeline steps with color coding")
    status_parser.add_argument("--show-completed", action="store_true", help="Include completed steps in the status output")
    status_parser.add_argument("--show-retired", action="store_true", help="Include retired steps in the status output")
    # retire action for marking jobs retired
    retire_parser = subparsers.add_parser("retire", parents=[common], help="Mark failed entries as retired")
    retire_range = retire_parser.add_mutually_exclusive_group()
    retire_parser.add_argument("--state", nargs="+",
    choices=["NONE", "PENDING", "RUNNING", "REQUEUED", "COMPLETING", "FAILED", "CANCELLED", "TIMEOUT", "COMPLETED"],
    default=["FAILED", "CANCELLED", "TIMEOUT"],
    help="SLURM job states that trigger retirement (default: FAILED, CANCELLED, TIMEOUT)")
    retire_range.add_argument("--all", action="store_true", help="Mark all non-retired jobs as retired, filtered by --state")
    retire_range.add_argument("--job", nargs="+", type=parse_job_id, metavar="ID", help="Retire specific jobs by SLURM job ID or entry job_id, space-separated (e.g. --job 24170103 24170106)")
    retire_range.add_argument("--step", nargs="+", type=int, help="Mark specific pipeline steps as retired (e.g. --step 1 2 3)")
    # cancel action for cancelling jobs
    cancel_parser = subparsers.add_parser("cancel", parents=[common], help="Cancel active jobs of the pipeline entries")
    cancel_range = cancel_parser.add_mutually_exclusive_group()
    cancel_range.add_argument("--all", action="store_true", help="Cancel all non-retired jobs, filtered by --state")
    cancel_range.add_argument("--job", nargs="+", type=parse_job_id, metavar="ID", help="Cancel specific jobs by SLURM job ID or entry job_id, space-separated (e.g. --job 24170104 24170107)")
    cancel_range.add_argument("--step", nargs="+", type=int, help="Cancel specific pipeline steps (e.g. --step 1 2 3)")
    cancel_parser.add_argument("--state", nargs="+",
    choices=["NONE", "PENDING", "RUNNING", "REQUEUED", "COMPLETING", "FAILED", "CANCELLED", "TIMEOUT", "COMPLETED"],
    default=["PENDING", "RUNNING", "REQUEUED", "COMPLETING"],
    help="SLURM job states that trigger cancellation (default: PENDING, RUNNING, REQUEUED, COMPLETING)")
    # Parse the arguments
    ARG = parser.parse_args()
    if ARG.subcommand == "retire" or ARG.subcommand == "cancel":
        if not (ARG.all or ARG.job or ARG.step):
            ARG.all = True  # Default to retiring all if no specific range is provided
    if ARG.verbose:
        print_arguments(ARG)
    directory_tree = resolve_directory_tree(ARG)
    if ARG.verbose:
        print(f"ROOT DIR: {directory_tree.root}")

    pipeline_entries_store = None
    if ARG.subcommand in ENTRY_SUBCOMMANDS:
        pipeline_entries_store = PipelineEntriesStore([processing_unit_subdir.abs for processing_unit_subdir in directory_tree.processing_unit_subdirs], topological_sort=(ARG.subcommand == "status"))
        if ARG.subcommand == "status":
            pipeline_entries_store.refresh(update_retired=ARG.show_retired, update_negative_step=True, filter_terminal_states=True)
        elif ARG.subcommand in ("retire", "cancel"):
            # Entries stored in a final state cannot change any more; query only the others.
            pipeline_entries_store.refresh(update_retired=False, update_negative_step=True, filter_terminal_states=True)
        elif ARG.subcommand == "log":
            def matches(entry):
                return ARG.id in (str(entry.get("job_id", "")), str(entry.get("slurm_id", "")))
            pipeline_entries_store.refresh(select=lambda subdir_abs, PIPELINE_ENTRIES: [entry for entry in PIPELINE_ENTRIES if matches(entry)])
        if ARG.verbose:
            from laupy.slurm import QUERY_COUNTS
            print(f"SLURM queries: {QUERY_COUNTS}")

    for processing_unit_subdir in directory_tree.processing_unit_subdirs:
        if ARG.verbose:
            print(f"Processing {processing_unit_subdir.rel} (abs: {processing_unit_subdir.abs})")
        SUBDIR_ABS = processing_unit_subdir.abs
        SUBDIR_REL = processing_unit_subdir.rel
        PIPELINE_DIR = os.path.join(SUBDIR_ABS, "pipeline")
        PIPELINE_LOG_DIR = os.path.join(PIPELINE_DIR, "log")
        PIPELINE_ENTRIES = pipeline_entries_store[SUBDIR_ABS] if pipeline_entries_store is not None else None
        if ARG.subcommand == "status":
            if len(PIPELINE_ENTRIES) == 0:
                print(f"No pipeline entries found for {SUBDIR_REL}.")
            else:
                output_lines = [f"{SUBDIR_REL} pipeline status:"]
                for entry in PIPELINE_ENTRIES:
                    dag_node_name = entry.get("dag_node_name", None)
                    step = entry.get("step", "N/A")
                    slurm_id = entry.get("slurm_id", None)
                    job_id = entry.get("job_id", "N/A")
                    entry_info = entry.get("slurm_info", {"State": "UNKNOWN"})
                    if slurm_id is None:
                        slurm_id = job_id  # Fallback to job_id if slurm_id is not present
                    job_state = entry_info.get("State", "UNKNOWN")
                    job_name = entry_info.get("JobName", "N/A")
                    basic_info = f"\tDAG node/step: {dag_node_name}, Slurm ID: {slurm_id}, Job Name: {job_name}, State: {job_state}"
                    retired = entry.get("retired", False)
                    if retired == True and not ARG.show_retired:
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
                         node_list = entry_info.get("NodeList", "N/A")
                         output_lines.append(colored(f"{basic_info}, Time limit: {time_limit}, Elapsed: {elapsed}, {node_list}", "green"))
                    elif job_state in ("REQUEUED", "COMPLETING"):
                        output_lines.append(f"{basic_info}")
                    elif job_state in ("FAILED", "CANCELLED", "TIMEOUT"):
                        output_lines.append(colored(f"{basic_info}", "red"))
                    elif job_state in ("COMPLETED"):
                        if ARG.show_completed:
                            elapsed = entry_info.get("Elapsed", "N/A")
                            output_lines.append(colored(f"{basic_info}, Elapsed: {elapsed}", "magenta"))
                    elif job_state not in ("COMPLETED"):
                        output_lines.append(colored(f"{basic_info} (unexpected state)", "yellow"))
                if len(output_lines) > 1:
                    print("\n".join(output_lines))
        if ARG.subcommand == "retire":
            if len(PIPELINE_ENTRIES) == 0:
                print(f"No pipeline entries found for {SUBDIR_REL}.")
            else:
                # Only failure states are ever retired, whatever --state lists.
                retire_states = [state for state in ARG.state if state in RETIRABLE_STATES]
                candidates = select_pipeline_entries(PIPELINE_ENTRIES, retire_states, job_ids=ARG.job, steps=ARG.step)
                output_lines = [f"{SUBDIR_REL} pipeline retire{' (dry run)' if ARG.dry_run else ''}:"]
                if ARG.dry_run:
                    for entry in candidates:
                        output_lines.append(f"{job_basic_info(entry)} -> Would be retired")
                else:
                    retired = retire_pipeline_entries(PIPELINE_ENTRIES, [entry["job_id"] for entry in candidates], reason="pipeline retire")
                    for entry in retired:
                        output_lines.append(colored(f"{job_basic_info(entry)} -> Marked as retired", "red"))
                if len(output_lines) > 1:
                    print("\n".join(output_lines))
        if ARG.subcommand == "cancel":
            if len(PIPELINE_ENTRIES) == 0:
                print(f"No pipeline entries found for {SUBDIR_REL}.")
            else:
                # Only active states are ever cancelled, whatever --state lists.
                cancel_states = [state for state in ARG.state if state in CANCELLABLE_STATES]
                candidates = select_pipeline_entries(PIPELINE_ENTRIES, cancel_states, job_ids=ARG.job, steps=ARG.step)
                if ARG.dry_run:
                    output_lines = [f"{SUBDIR_REL} pipeline cancel (dry run):"]
                    for entry in candidates:
                        output_lines.append(f"{job_basic_info(entry)} -> Would be cancelled")
                    if len(output_lines) > 1:
                        print("\n".join(output_lines))
                    continue
                # SLURM info was refreshed above for all processing units, no need to query it again.
                results = cancel_pipeline_entries(PIPELINE_ENTRIES, [entry["job_id"] for entry in candidates], refresh=False)
                output_lines = [f"{SUBDIR_REL} pipeline cancel:"]
                for result in results:
                    entry = result["entry"]
                    if result["slurm_id"] is None:
                        output_lines.append(colored(f"\tNode: {entry.get('dag_node_name')}, Job ID: {entry.get('job_id', 'N/A')} -> {result['error']}", "red"))
                    elif result["error"] is None:
                        output_lines.append(colored(f"{job_basic_info(entry, result['state'])} -> Cancelled successfully", "yellow"))
                    else:
                        output_lines.append(colored(f"{job_basic_info(entry, result['state'])} -> Failed to cancel: {result['error']}", "red"))
                if len(output_lines) > 1:
                    print("\n".join(output_lines))
        if ARG.subcommand == "log":
            selected = [entry for entry in PIPELINE_ENTRIES if ARG.id in (str(entry.get("job_id", "")), str(entry.get("slurm_id", "")))]
            if not ARG.stdout and not ARG.stderr:
                ARG.stdout = True
                ARG.stderr = True
            for entry in selected:
                step = entry.get("step", "N/A")
                job_id = entry.get("job_id", "N/A")
                slurm_info = entry.get("slurm_info", {})
                job_name = slurm_info.get("JobName", "N/A")
                job_state = slurm_info.get("State", "N/A")
                std_out_file = slurm_info.get("StdOut", "")
                std_err_file = slurm_info.get("StdErr", "")
                basic_info = f"\tStep: {step}, Job ID: {job_id}, Job Name: {job_name}, State: {job_state}, StdOut: {std_out_file}, StdErr: {std_err_file}"
                print(colored(f"{basic_info}", "yellow"))
                if ARG.stdout:
                    if std_out_file and os.path.exists(std_out_file):
                        print(colored("--- START SLURM STDOUT", "green"))
                        with open(std_out_file) as f:
                            print(f.read())
                        print(colored("--- END SLURM STDOUT", "green"))
                    else:
                        print(f"No STDOUT file found for Step {entry.get('step', 'N/A')}, Job ID {entry.get('job_id', 'N/A')}")
                if ARG.stderr:
                    if std_err_file and os.path.exists(std_err_file):
                        print(colored("--- START SLURM STDERR", "red"))
                        with open(std_err_file) as f:
                            print(f.read())
                        print(colored("--- END SLURM STDERR", "red"))
                    else:
                        print(f"No STDERR file found for Step {entry.get('step', 'N/A')}, Job ID {entry.get('job_id', 'N/A')}")
        if ARG.subcommand == "create":
            action = "Would create" if ARG.dry_run else "Creating"
            if os.path.isdir(PIPELINE_LOG_DIR):
                print(f"{SUBDIR_REL}: pipeline/log already exists")
            else:
                print(f"{SUBDIR_REL}: {action} pipeline/log")
                if not ARG.dry_run:
                    os.makedirs(PIPELINE_LOG_DIR, exist_ok=True)
        if ARG.subcommand == "clean":
            # Forget the entries, e.g. to resolve failed dependencies, without touching scripts or logs
            count = len(load_pipeline_entries(SUBDIR_ABS))
            action = "Would clean" if ARG.dry_run else "Cleaning"
            print(f"{SUBDIR_REL}: {action} pipeline/dag.json ({count} entries)")
            if not ARG.dry_run:
                clean_pipeline_entries(SUBDIR_ABS)
        if ARG.subcommand == "delete":
            if not os.path.isdir(PIPELINE_DIR):
                print(f"{SUBDIR_REL}: no pipeline directory")
            elif ARG.dry_run:
                print(f"{SUBDIR_REL}: Would delete {PIPELINE_DIR} (entries, scripts and logs)")
            else:
                print(f"{SUBDIR_REL}: Deleting {PIPELINE_DIR} (entries, scripts and logs)")
                remove_pipeline_dir(PIPELINE_DIR)

    if pipeline_entries_store is not None and ARG.dry_run and ARG.subcommand in ("retire", "cancel"):
        pipeline_entries_store = None  # dry run: write nothing, not even refreshed SLURM info
    if pipeline_entries_store is not None:
        # Writes only processing units whose entries changed (retired/cancelled flags or
        # refreshed slurm_info, which spares SLURM queries next time).
        try:
            saved = pipeline_entries_store.store()
            if ARG.verbose:
                print(f"Saved entries of {len(saved)} processing unit(s)")
        except OSError as e:
            if ARG.subcommand in ("retire", "cancel"):
                print(f"Error: cannot save pipeline entries: {e}", file=sys.stderr)
                sys.exit(1)
            # status/log only cache refreshed SLURM info; e.g. read-only projects are fine
            log.warning(f"Cannot save refreshed pipeline entries: {e}")


if __name__ == "__main__":
    main()
