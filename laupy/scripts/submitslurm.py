#!/usr/bin/env python

import os
import sys
import time
from laupy import maxwell
import shlex
import logging
from laupy.cli import LAUPY_PYTHON, join_dash_values, SubmissionEnvironment, job_path_variable, prepend_path_directories, workspace_path_directories, vanilla_base_variables, vanilla_kept_directories, check_python_environment, apply_node_source_arguments, print_node_notes, print_arguments, resolve_directory_tree
from laupy.submitslurm_options import build_parser
from laupy.pipeline_entries import PipelineEntriesStore, select_pipeline_entries, retire_pipeline_entries, cancel_pipeline_entries
from laupy.flow import SchedulingError, clean_pipeline_entries, remove_pipeline_dir, schedule_slurm_job
from laupy.slurm import run_slurm_command

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


RANGE_DESCRIPTIONS = {"cmd": "same command", "step": "same node", "jobname": "same job name"}


def quote_command(cmd):
    return " ".join(shlex.quote(str(arg)) for arg in cmd)


def dependency_description(PIPELINE_ENTRIES, dependency_slurm_ids):
    """', after 07detect.sbatch (123), 00bin.sbatch (124, 125)' for the SLURM IDs a new job waits for."""
    if not dependency_slurm_ids:
        return ""
    script_by_id = {str(e.get("slurm_id")): e.get("script_name") for e in PIPELINE_ENTRIES}
    by_script = {}  # insertion ordered: first appearance of each script
    for jid in dependency_slurm_ids:
        by_script.setdefault(script_by_id.get(str(jid)) or "job", []).append(str(jid))
    return ", after " + ", ".join(f"{script} ({', '.join(ids)})" for script, ids in by_script.items())


def range_entries(range_name, node_entries, cmd_entries, jobname_entries):
    """Entries of the submitted node covered by a --skip/--cancel/--retire-range value."""
    return {"cmd": cmd_entries, "step": node_entries, "jobname": jobname_entries}.get(range_name, [])


def main():
    # The option set is laupy.submitslurm_options.build_parser(), shared with programs composing calls
    parser = build_parser()

    # Parse the arguments
    # -a "--bin 2 ..." / --env A=-x: values starting with "-" are joined to their option first
    ARG = parser.parse_args(join_dash_values(sys.argv[1:]))
    print(f"submitslurm with parameters: {quote_command(sys.argv[1:])}")
    # --vanilla-strict is --vanilla without directories of the submitting PATH
    ARG.vanilla = ARG.vanilla or ARG.vanilla_strict
    # Environment of the jobs: inherited global environment + per-job variables
    ENVIRONMENT = SubmissionEnvironment(extra=dict(ARG.env), vanilla=ARG.vanilla)
    # Python environment of the jobs (LAUPY_PYTHON): --python, else --env LAUPY_PYTHON=..., else the submitting shell
    # (not with --vanilla, which takes nothing from the submitting environment).
    # It is always exported explicitly and recorded in the entries, so a resubmission is exact.
    if ARG.python is not None and ENVIRONMENT.extra.get(LAUPY_PYTHON, ARG.python) != ARG.python:
        print(f"WARNING: --python {ARG.python} overrides --env {LAUPY_PYTHON}={ENVIRONMENT.extra[LAUPY_PYTHON]}", file=sys.stderr)
    if ARG.job_path and "PATH" in ENVIRONMENT.extra:
        parser.error("--job-path and --env PATH=... both set the PATH of the jobs; use one")
    if ARG.python is not None:
        LAUPY_PYTHON_VALUE, LAUPY_PYTHON_SOURCE = ARG.python, "--python"
    elif ENVIRONMENT.extra.get(LAUPY_PYTHON):
        LAUPY_PYTHON_VALUE, LAUPY_PYTHON_SOURCE = ENVIRONMENT.extra[LAUPY_PYTHON], f"--env {LAUPY_PYTHON}"
    elif not ARG.vanilla and ENVIRONMENT.global_env.get(LAUPY_PYTHON):
        LAUPY_PYTHON_VALUE, LAUPY_PYTHON_SOURCE = ENVIRONMENT.global_env[LAUPY_PYTHON], "submitting shell"
    else:
        LAUPY_PYTHON_VALUE, LAUPY_PYTHON_SOURCE = None, None
    if LAUPY_PYTHON_VALUE is not None:
        try:
            LAUPY_PYTHON_VALUE = check_python_environment(LAUPY_PYTHON_VALUE)
        except ValueError as e:
            parser.error(f"{LAUPY_PYTHON} from {LAUPY_PYTHON_SOURCE}: {e}")
        ENVIRONMENT.extra[LAUPY_PYTHON] = LAUPY_PYTHON_VALUE
        print(f"Python environment of the jobs ({LAUPY_PYTHON}, from {LAUPY_PYTHON_SOURCE}): {LAUPY_PYTHON_VALUE}")
    if ARG.verbose:
        print_arguments(ARG)
        for line in ENVIRONMENT.global_lines():
            print(line)

    pipeline_mode = ARG.create_pipeline_only or ARG.clean_dag or ARG.delete_pipeline_only
    if not pipeline_mode and ARG.scriptName is None:
        parser.error("scriptName is required unless using --create-pipeline-only, --clean-dag, or --delete-pipeline-only.")
    directory_tree = resolve_directory_tree(ARG)
    if ARG.vanilla:
        try:
            ENVIRONMENT.base = vanilla_base_variables(directory_tree.root, directory_tree.sbatch_abs, ENVIRONMENT.global_env, ARG.job_path, strict=ARG.vanilla_strict)
        except ValueError as e:
            parser.error(f"--vanilla: {e}")
        print(("Strict vanilla" if ARG.vanilla_strict else "Vanilla") + " submission: the submitting environment is not propagated; every job gets only SLURM_* and")
        for name, value in ENVIRONMENT.base.items():
            print(f"    {name}={value}")
        print("    plus PIPELINE_SCRIPTNAME, PIPELINE_JOBNAME, LD_PRELOAD=" + (", " + ", ".join(ENVIRONMENT.extra) if ENVIRONMENT.extra else ""))
        if not ARG.vanilla_strict:
            missing = vanilla_kept_directories(ENVIRONMENT.global_env)[1]
            if missing:
                print("    WARNING: not on the submitting PATH, so their directories are not on the job PATH: " + ", ".join(missing), file=sys.stderr)
    # ROOT/bin (initPython, reconstructSlice2D, ...), ROOT/script and the SLURM script directory always lead the job PATH
    leading = workspace_path_directories(directory_tree.root, directory_tree.sbatch_abs)
    try:
        if "PATH" in ENVIRONMENT.extra:
            ENVIRONMENT.extra["PATH"] = prepend_path_directories(leading, ENVIRONMENT.extra["PATH"])
            print(f"Job PATH: {os.pathsep.join(leading)}, then --env PATH")
        elif not ARG.vanilla:
            ENVIRONMENT.extra["PATH"] = job_path_variable(ARG.job_path or [], ENVIRONMENT.global_env, leading)
            print(f"Job PATH: {os.pathsep.join(leading + (ARG.job_path or []))}, then the inherited PATH (exported explicitly)")
    except ValueError as e:
        parser.error(f"job PATH: {e}")
    for directory in ARG.job_path or []:
        if not os.path.isdir(directory):
            print(f"WARNING: --job-path directory does not exist: {directory}", file=sys.stderr)

    #If --partition is not set by the user and --gpu-nodes is requested, set partition to "allgpu"
    partition_was_user_set = any(arg in ("-f", "--partition") for arg in sys.argv)
    if ARG.gpu_nodes and not partition_was_user_set:
        ARG.partition = "allgpu"
        print("GPU nodes requested without explicit -f/--partition, setting partition to 'allgpu'")

    # Check if script exists
    if not pipeline_mode:
        SCRIPTNAME = ARG.scriptName
        SCRIPT_ABS = os.path.join(directory_tree.sbatch_abs, SCRIPTNAME)
        SCRIPT_REL = os.path.join(directory_tree.sbatch_rel, SCRIPTNAME)
        if not os.path.isfile(SCRIPT_ABS):
            print(f"Script {SCRIPT_REL} does not exist.", file=sys.stderr)
            sys.exit(1)

    SLURM_ARGS_LIST = []
    # Node selection: classes and node states from one cached sinfo query (laupy.maxwell)
    apply_node_source_arguments(parser, ARG)	# Only configures; SLURM is queried when nodes are needed
    if ARG.nodelist or ARG.gpu_nodes or ARG.excellent_nodes or ARG.strong_nodes or ARG.exclude_weak_nodes:
        try:
            node_request = maxwell.sbatch_node_request(ARG.partition, nodelist=ARG.nodelist, gpu_only=ARG.gpu_nodes,
                                                       excellent=ARG.excellent_nodes, strong=ARG.strong_nodes,
                                                       exclude_weak=ARG.exclude_weak_nodes)
        except RuntimeError as e:
            print_node_notes()
            print(f"Cannot query the nodes: {e}", file=sys.stderr)
            sys.exit(1)
        print_node_notes()
        if ARG.verbose:
            source = maxwell.get_node_source()
            print(f"Node hardware from: {source['hardware_from'] or 'nothing'} (feature provider {source['feature_provider']}"
                  + (f", node table {source['table']})" if source["table"] else ", no node table)"))
            for line in maxwell.format_query_log():
                print(line)
        for note in node_request.notes:
            print(note)
        if node_request.error:
            # An explicit node list or GPU request must not silently run elsewhere
            print(f"{node_request.error}, not submitting.", file=sys.stderr)
            sys.exit(1)
        SLURM_ARGS_LIST.extend(node_request.sbatch_arguments())
        if ARG.verbose:
            if node_request.nodelist:
                print(f"Submitting to nodes: {maxwell.compress_node_list(node_request.nodelist)}")
            if node_request.exclude:
                print(f"Excluding nodes: {maxwell.compress_node_list(node_request.exclude)}")
    else:
        if ARG.node_table or ARG.no_list or ARG.feature_provider:
            print("Note: --list/--no-list/--feature-provider have no effect without -n/-g/-e/-s/-x (no node query needed).")
        if ARG.verbose:
            print("Submitting without node restrictions.")

    if ARG.oversubscribe:
        SLURM_ARGS_LIST.append("--oversubscribe")

    SLURM_ARGS_LIST.append(f"--partition={ARG.partition}")
    print(f"ROOT DIR: {directory_tree.root}")
    if ARG.verbose and not pipeline_mode:
        print(f"SBATCH OPTIONS: {quote_command(SLURM_ARGS_LIST)}")

    # DAG node of the submission (resolved once for all processing units)
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

    # PIPELINE_ENTRIES of all processing units with one batched SLURM query for the
    # submitted node and its ancestors (dependencies, skip/retire/cancel checks).
    pipeline_entries_store = None
    if dag_node_name is not None and not ARG.dry_run:
        pipeline_entries_store = PipelineEntriesStore([processing_unit_subdir.abs for processing_unit_subdir in directory_tree.processing_unit_subdirs])
        pipeline_entries_store.refresh(limit_to_ancestors_including_node=dag_node_name, update_retired=False, update_negative_step=False, filter_terminal_states=True, node_dependencies=dependencies)
        if ARG.verbose:
            from laupy.slurm import QUERY_COUNTS
            print(f"SLURM queries: {QUERY_COUNTS}")

    for processing_unit_subdir in directory_tree.processing_unit_subdirs:
        subdir = processing_unit_subdir.name
        SUBDIR_ABS = processing_unit_subdir.abs
        SUBDIR_REL = processing_unit_subdir.rel
        # Create job name
        if not pipeline_mode:
            SCRIPTNAME_NOEXT = os.path.splitext(SCRIPTNAME)[0]
            JOBNAME = f"{SCRIPTNAME_NOEXT}_{subdir}"
            JOB_VARS = ENVIRONMENT.job_vars(SCRIPTNAME, JOBNAME)
            if ARG.delaytime > 0:
                time.sleep(ARG.delaytime)
            SLURM_CMD = ["sbatch"] + SLURM_ARGS_LIST
            SLURM_CMD += [f"--job-name={JOBNAME}"]
            SLURM_CMD += [ENVIRONMENT.export_option(JOB_VARS)]
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
            # Job block: the script command relative to ROOT DIR; --verbose adds the full sbatch command
            print(f"\nJob {JOBNAME} in {SUBDIR_REL}:")
            print(f"    CMD: {quote_command(EXEC_CMD_REL)}")
            if ARG.verbose:
                print(f"    ENV: {ENVIRONMENT.describe_job_vars(JOB_VARS)}")
                print(f"    SBATCH: {quote_command(SLURM_CMD_REL)}")
        if ARG.dry_run:
            if not pipeline_mode:
                print("    INFO: dry run, not submitted")
            continue
        PIPELINE_DIR = os.path.join(SUBDIR_ABS, "pipeline")
        PIPELINE_LOG_DIR = os.path.join(PIPELINE_DIR, "log")
        if ARG.delete_pipeline_only:
            print(f"{SUBDIR_REL}: deleting pipeline directory (entries, scripts and logs)")
            remove_pipeline_dir(PIPELINE_DIR)
            continue
        if ARG.clean_dag:
            # Forget the entries to resolve failed dependencies without creating new pipeline scripts or submitting jobs
            print(f"{SUBDIR_REL}: forgetting pipeline entries (pipeline/dag.json)")
            clean_pipeline_entries(SUBDIR_ABS)
            continue
        os.makedirs(PIPELINE_LOG_DIR, exist_ok=True)
        if ARG.create_pipeline_only:
            print(f"{SUBDIR_REL}: pipeline/log ready")
            continue
        if dag_node_name is None:
            # --pipeline-step -1: plain submission outside the DAG
            slurm_id = run_slurm_command(SLURM_CMD, cwd=str(directory_tree.root), env=ENVIRONMENT.global_env)
            print(f"    INFO: submitted {slurm_id}, outside the DAG" if slurm_id is not None else "    ERROR: not submitted, sbatch failed (see above)")
            continue

        PIPELINE_ENTRIES = pipeline_entries_store[SUBDIR_ABS]
        # Non-retired entries of the submitted node, by scope, to evaluate skip, cancel and retire
        node_entries = [entry for entry in PIPELINE_ENTRIES if entry.get("dag_node_name") == dag_node_name and "job_id" in entry and not entry.get("retired", False)]
        cmd_entries = [entry for entry in node_entries if entry.get("command") == " ".join(EXEC_CMD_ABS)]
        jobname_entries = [entry for entry in node_entries if entry.get("slurm_info", {}).get("JobName", "") == JOBNAME]
        # Skip if an entry in scope is still in one of --skip-state
        skip_entries = select_pipeline_entries(range_entries(ARG.skip_range, node_entries, cmd_entries, jobname_entries), ARG.skip_state)
        if skip_entries:
            found = ", ".join(f"{entry.get('slurm_id') or entry['job_id']} {entry.get('slurm_info', {}).get('State', '')}" for entry in skip_entries)
            print(f"    INFO: skipped, {RANGE_DESCRIPTIONS.get(ARG.skip_range, ARG.skip_range)} already queued or running ({found})")
            continue
        try:
            # Cancel active jobs in --cancel-range/--cancel-state before submitting the replacement
            cancel_candidates = select_pipeline_entries(range_entries(ARG.cancel_range, node_entries, cmd_entries, jobname_entries), ARG.cancel_state)
            cancelled = []
            for result in cancel_pipeline_entries(PIPELINE_ENTRIES, [entry["job_id"] for entry in cancel_candidates], refresh=False):
                if result["error"] is None:
                    print(f"    INFO: cancelled {result['slurm_id']} ({result['state']}, {RANGE_DESCRIPTIONS.get(ARG.cancel_range)})")
                    cancelled.append(result["entry"]["job_id"])
                else:
                    print(f"    WARNING: could not cancel {result['slurm_id']}: {result['error']}")
            retire_candidates = select_pipeline_entries(range_entries(ARG.retire_range, node_entries, cmd_entries, jobname_entries), ARG.retire_state)
            NEW_ENTRY = schedule_slurm_job(PIPELINE_ENTRIES, dag_node_name, SCRIPTNAME, JOBNAME, SUBDIR_ABS, SLURM_CMD_ABS, EXEC_CMD_ABS, node_dependencies=dependencies, env=ENVIRONMENT.global_env, raise_errors=True, laupy_python=LAUPY_PYTHON_VALUE)
            PIPELINE_ENTRIES.append(NEW_ENTRY)
            print(f"    INFO: submitted {NEW_ENTRY['slurm_id']}, node {dag_node_name}{dependency_description(PIPELINE_ENTRIES, NEW_ENTRY.get('dependency_slurm_ids'))}")
            # The replaced entries are retired and point to the entry that replaced them
            retire_pipeline_entries(PIPELINE_ENTRIES, cancelled, retired_by=NEW_ENTRY["job_id"],
                           reason=f"Cancelled due to new submission with --cancel-range {ARG.cancel_range} and --cancel-state {ARG.cancel_state}")
            retired = retire_pipeline_entries(PIPELINE_ENTRIES, [entry["job_id"] for entry in retire_candidates], retired_by=NEW_ENTRY["job_id"],
                                     reason=f"Retired due to new submission with --retire-range {ARG.retire_range} and --retire-state {ARG.retire_state}")
            if retired:
                states = sorted({entry.get("slurm_info", {}).get("State", "?") for entry in retired})
                print(f"    INFO: retired {len(retired)} earlier entr{'y' if len(retired) == 1 else 'ies'} ({', '.join(states)}, {RANGE_DESCRIPTIONS.get(ARG.retire_range)})")
            appendCommand(EXEC_CMD_ABS, os.path.join(SUBDIR_ABS, "pipeline", "exec.sh"))
        except SchedulingError as e:
            print(f"    ERROR: not submitted, {e}")
        except Exception as e:
            print(f"    ERROR: not submitted, {e.__class__.__name__}: {e}")
        finally:
            # Persist each subdir right away: new entry, retirements, cancellations
            pipeline_entries_store.store([SUBDIR_ABS])


if __name__ == "__main__":
    main()
