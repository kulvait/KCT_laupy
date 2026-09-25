#!/usr/bin/env python

import os
import json
import networkx as nx
from laupy import slurm

# Functions for management of pipeline
from typing import Union, List, Dict
from laupy.slurm import run_slurm_command, get_active_slurmids
import subprocess
import logging
import uuid
import time

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


def update_dag_entries(DAG: list, pipeline_step: int = -1, update_retired: bool = False, update_negative_step: bool = False, filter_terminal_states: bool = True) -> None:
    """Update DAG entries with current SLURM info for active dependencies and same step entries to evaluate skip conditions and retirement.
       Updates all entries with "step" < pipeline_step, for negative pipeline_step, updates all entries."""
    DAG_flt = DAG
    if not update_negative_step:
        DAG_flt = [ entry for entry in DAG_flt if entry["step"] >=0  ]
    if not update_retired:
        DAG_flt = [ entry for entry in DAG_flt if not entry.get("retired", False) ]
    if pipeline_step >= 0:
        DAG_flt = [ entry for entry in DAG_flt if entry["step"] <= pipeline_step ]
    if filter_terminal_states:
        #If entry is in terminal state and has already its slurm_info, do not query it
        DAG_flt = [ entry for entry in DAG_flt if not ( "slurm_info" in entry and entry["slurm_info"]["State"] in ["COMPLETED", "FAILED", "CANCELLED"] ) ]
    #Get entries of potential dependencies and same step entries and same step and command entries to evaluate skip conditions and retirement
    job_ids = [ entry["job_id"] for entry in DAG_flt]
    slurm_ids = []
    for jid in job_ids:
        entry = next((entry for entry in DAG_flt if entry["job_id"] == jid), None)
        if entry is not None and "slurm_id" in entry:
            slurm_ids.append(entry["slurm_id"])
        else:
            slurm_ids.append(jid)  # Fallback to job_id if slurm_id is not available
    # slurm_ids = [ entry["slurm_id"] for entry in DAG_flt] ... use after transition to slurm_id for all entries
    if len(slurm_ids) > 0:
        slurm_infos = slurm.slurm_info(slurm_ids)
        for i in range(len(slurm_ids)):
            jid = slurm_ids[i]
            info = slurm_infos[i]
            if info["State"] == "UNKNOWN":
                continue
            DAG_flt[i]["slurm_info"] = info

def load_dag(execution_unit_dir: str) -> list:
    """Load the DAG (Directed Acyclic Graph) for the execution context."""
    dag_file = os.path.join(execution_unit_dir, "pipeline", "dag.json")
    if os.path.exists(dag_file):
        with open(dag_file) as f:
            return json.load(f)
    return []

def save_dag(execution_unit_dir: str, dag: list) -> None:
    """Save the DAG (Directed Acyclic Graph) for the execution context."""
    dag_file = os.path.join(execution_unit_dir, "pipeline", "dag.json")
    os.makedirs(os.path.dirname(dag_file), exist_ok=True)
    with open(dag_file, "w") as f:
        json.dump(dag, f, indent=2)

def clean_dag(execution_unit_dir: str) -> None:
    """Clear the DAG (Directed Acyclic Graph) for the execution context by resetting it to an empty list."""
    save_dag(execution_unit_dir, [])

def string_is_integer(s: str) -> bool:
    """Check if a string can be converted to an integer."""
    try:
        int(s)
        return True
    except ValueError:
        return False

def constructDAG(DAG_ENTRIES: list) -> nx.DiGraph:
    """
    Construct a DAG from DAG_ENTRIES.

    Rules:
      - Numeric node names (>= 0) form an implicit chain:
            0 -> 1 -> 2 -> ...
        Numeric nodes may not have explicit dependencies.
      - Named nodes use their 'dag_node_deps' list.
      - All dependencies must refer to existing nodes, otherwise they will be ignored with a warning. 
      - A named node with no dependencies starts a new independent branch from dag_root.
      - The resulting graph must be acyclic.
    """

    dag = nx.DiGraph()
    dag.add_node("dag_root")  # Add a root node for the DAG
    dag.add_node("0")  # Add the initial numeric node
    dag.add_edge("dag_root", "0")  # Connect the root to the initial numeric node

    # First add all nodes    
    max_numeric_node = 0
    for entry in DAG_ENTRIES:
        dag_node_name = entry.get("dag_node_name", None)
        if dag_node_name is None:
            dag_node_name = str(entry.get("step", 0))  # Fallback to step if dag_node_name is not provided
        dag_node_name = str(dag_node_name)  # Normalize to string for consistent handling
        entry["dag_node_name"] = dag_node_name  # Update the entry with the normalized name
        node = dag_node_name
        try:
            numeric_node = int(node)
            if numeric_node < 0:
                continue  # Skip negative numeric nodes
            max_numeric_node = max(max_numeric_node, numeric_node)
        except ValueError:
            dag.add_node(node)  # Add named nodes directly

    #Make chain of existing numeric nodes
    for i in range(max_numeric_node):
        dag.add_edge(str(i), str(i + 1)) # Nodes are automatically created when adding edges, so no need to add them explicitly

    # Add dependencies
    for entry in DAG_ENTRIES:
        dag_node_name = entry.get("dag_node_name", None)
        node = str(dag_node_name)  # Normalize to string for consistent handling
        dependencies = [str(dep) for dep in entry.get("dag_node_deps", [])]
        invalid_deps = [dep for dep in dependencies if str(dep) not in dag]
        if invalid_deps:
            log.warning(f"Node '{node}' has dependencies that are not in the DAG: {invalid_deps}. These will be ignored.")
        dependencies = [str(dep) for dep in dependencies if str(dep) in dag]  # Filter out dependencies that are not in the DAG
        if not string_is_integer(node):
            #For named nodes, add edges based on dependencies
            for dep in dependencies:
                dag.add_edge(dep, node)
            # If no dependencies are specified, connect to the root node
            if len(dependencies) == 0:
                dag.add_edge("dag_root", node)
    # Validate DAG
    if not nx.is_directed_acyclic_graph(dag):
        cycle = nx.find_cycle(dag)
        raise ValueError(f"DAG contains a cycle: {cycle}")
    return dag

def resubmit_slurm_job(DAG_ENTRIES: list, entry_id: Union[str, int, Dict]):
    """Resubmit a SLURM job based on the provided DAG entry and update the DAG with the new job ID."""
    try:
        if isinstance(entry_id, dict):
            DAG_ENTRY = entry_id
        elif isinstance(entry_id, int):
            DAG_ENTRY = next((entry for entry in DAG_ENTRIES if entry.get("slurm_id") == entry_id), None)
        elif isinstance(entry_id, str):
            DAG_ENTRY = next((entry for entry in DAG_ENTRIES if str(entry.get("job_id")) == entry_id), None)
        # First cancel and retire the existing job if it has a valid SLURM ID
        if "slurm_id" in DAG_ENTRY and DAG_ENTRY["slurm_id"] is not None:
            slurm_info = DAG_ENTRY.get("slurm_info", {})
            slurm_state = slurm_info.get("State", "UNKNOWN")
            if slurm_state not in ["COMPLETED", "FAILED", "CANCELLED"]:
                subprocess.run(["scancel", str(DAG_ENTRY["slurm_id"])], check=False)
                log.info(f"Cancelled SLURM job with ID {DAG_ENTRY['slurm_id']} for node '{DAG_ENTRY['dag_node_name']}'")
        DAG_ENTRY["retired"] = True  # Mark the old entry as retired
        JOB_ID = uuid.uuid4().hex
        ROOTDIR = os.path.dirname(os.path.dirname(os.path.abspath(DAG_ENTRY["execution_unit_dir"])))
        SLURM_CMD = DAG_ENTRY["slurm_command"].copy()  # Create a copy of the SLURM command to avoid modifying the original
        NEW_DAG_ENTRY = schedule_slurm_job(DAG_ENTRIES=DAG_ENTRIES, dag_node_or_step_name=DAG_ENTRY["dag_node_name"], script_name=DAG_ENTRY["script_name"], job_name=DAG_ENTRY["job_name"], execution_unit_dir=DAG_ENTRY["execution_unit_dir"], slurm_cmd=SLURM_CMD, cmd=[DAG_ENTRY["command"]], dag_dependencies=DAG_ENTRY.get("dag_node_deps", []))
        return NEW_DAG_ENTRY
    except Exception as e:
        log.error(f"Error resubmitting SLURM job for node '{DAG_ENTRY['dag_node_name']}': {e}")
        return None

def schedule_slurm_job(DAG_ENTRIES: list, dag_node_or_step_name: str, script_name: str, job_name: str, execution_unit_dir: str, slurm_cmd: list, cmd: list, dag_dependencies: list = None):
    """Submit a SLURM job based on the provided job entry and update the DAG with the job ID."""
    try:
        for entry in DAG_ENTRIES:
            dag_node_name = entry.get("dag_node_name", None)
            # START To remove 
            if dag_node_name is None:
                dag_node_name = str(entry.get("step", 0))  # Fallback to step if dag_node_name is not provided
            slurm_id = entry.get("slurm_id", None)
            if slurm_id is None:
                entry["slurm_id"] = entry["job_id"]  # Fallback to job_id if slurm_id is not provided
            # END To remove
        JOB_ID = uuid.uuid4().hex
        ROOTDIR = os.path.dirname(os.path.dirname(os.path.abspath(execution_unit_dir)))
        dag_node_or_step_name = str(dag_node_or_step_name)  # Normalize to string for consistent handling
        numeric_node = False
        negative_node = False
        SLURM_CMD = slurm_cmd.copy()  # Create a copy of the SLURM command to avoid modifying the original
        DAG_ENTRY = {}
        DAG_ENTRY["script_name"] = script_name
        DAG_ENTRY["job_name"] = job_name
        DAG_ENTRY["dag_node_name"] = dag_node_or_step_name
        DAG_ENTRY["command"] = "".join(cmd)
        DAG_ENTRY["slurm_command"] = slurm_cmd
        DAG_ENTRY["execution_unit_name"] = os.path.basename(execution_unit_dir)
        DAG_ENTRY["execution_unit_dir"] = execution_unit_dir
        if string_is_integer(dag_node_or_step_name):
            step = int(dag_node_or_step_name)
            DAG_ENTRY["step"] = step
            DAG_ENTRY["dag_node_deps"] = []  # Numeric nodes do not have explicit
            numeric_node = True
            if step < 0:
                negative_node = True
        else:
            DAG_ENTRY["step"] = -2  # Use -2 for named nodes
            DAG_ENTRY["dag_node_deps"] = dag_dependencies if dag_dependencies else []
        if not negative_node:
            DAG = constructDAG(DAG_ENTRIES + [DAG_ENTRY])
            #Get all ancestors
            ancestors = nx.ancestors(DAG, dag_node_or_step_name)
            dependencies = [entry for entry in DAG_ENTRIES if str(entry["dag_node_name"]) in ancestors and entry.get("retired", False) == False and "slurm_id" in entry and entry.get("slurm_info", {}).get("State", "UNKNOWN") not in ["COMPLETED"] ]
            print(f"Scheduling SLURM job '{job_name}' for node '{dag_node_or_step_name}' with dependencies: {[entry['script_name'] for entry in dependencies]}")
            active_dependencies = get_active_slurmids(dependencies, raise_on_fail=True)
            if len(active_dependencies) > 0:
                SLURM_CMD.insert(1, f"--dependency=afterok:{':'.join(str(jid) for jid in active_dependencies)}")
        # For negative nodes, run the command directly without submitting to SLURM
        slurm_id = run_slurm_command(SLURM_CMD, cwd=ROOTDIR)
        DAG_ENTRY["job_id"] = JOB_ID
        DAG_ENTRY["slurm_id"] = slurm_id
        DAG_ENTRY["retired"] = False
        DAG_ENTRY["timestamp"] = time.time()
        return DAG_ENTRY
    except Exception as e:
        log.error(f"Error scheduling SLURM job {job_name} with step/dag_node '{dag_node_or_step_name}': {e}")
        log.error(f"{e.__class__.__name__} on line {e.__traceback__.tb_lineno} in {__file__}")
        return None

