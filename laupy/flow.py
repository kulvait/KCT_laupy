#!/usr/bin/env python

import os
import json
from laupy import slurm

# Functions for management of pipeline
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
    slurm_ids = [ entry["job_id"] for entry in DAG_flt if "job_id" in entry ]
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
