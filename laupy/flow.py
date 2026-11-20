#!/usr/bin/env python

import os
import json
#Avoid the following import as it imports whole networkx and can take long.
#import networkx as nx
#from networkx.algorithms.dag import ancestors as nx_ancestors, is_directed_acyclic_graph as nx_is_directed_acyclic_graph
#from networkx.algorithms.cycles import find_cycle as nx_find_cycle
#from networkx.classes.digraph import DiGraph as nx_DiGraph
from laupy.graph_utils import (ancestors as nx_ancestors,
			 is_directed_acyclic_graph as nx_is_directed_acyclic_graph,
			 find_cycle as nx_find_cycle,
			 DirectedGraph as nx_DiGraph,
			 topological_sort as nx_topological_sort)
from laupy import slurm

# Functions for management of pipeline
from typing import Union, List, Dict
from laupy.slurm import run_slurm_command, get_active_slurmids, TERMINAL_STATES
from pathlib import Path
import tempfile
import logging
import uuid
import time

# Create a logger specific to this module
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)# Set the logging level to INFO
# Create a console handler and set its level to INFO
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s : %(message)s', datefmt='%d.%m.%Y %H:%M:%S')
ch.setFormatter(formatter)
# Add the handler to the logger
log.addHandler(ch)
log.propagate = False  # Prevent log messages from being propagated to the root logger


# ----------------------------------------------------------------------
# Terminology
#
# PIPELINE_ENTRIES: list of entry dicts stored in <processing_unit_subdir>/pipeline/dag.json.
#   Each entry is one scheduled SLURM job (job_id, slurm_id, slurm_info,
#   retired, ...) attached to a DAG node by dag_node_name/dag_node_deps.
#   Several entries may belong to the same node (resubmissions).
# DAG: the directed acyclic graph of nodes, built from PIPELINE_ENTRIES by constructDAG().
# ----------------------------------------------------------------------

def pipeline_entries_to_refresh(PIPELINE_ENTRIES: list, limit_to_ancestors_including_node: str = "-1", update_retired: bool = False, update_negative_step: bool = False, filter_terminal_states: bool = True, node_dependencies: list = None) -> list:
	"""Select the entries whose SLURM info update_pipeline_entries() would query.

	limit_to_ancestors_including_node: only entries of that DAG node and its
	ancestors ("-1": all entries). Ancestors are computed on the full DAG, so a
	finished node in the middle of a chain does not hide the nodes above it.
	update_negative_step: include entries with step -1 (submitted outside the DAG).
	update_retired: include retired entries.
	filter_terminal_states: skip entries whose stored state is final (TERMINAL_STATES).
	"""
	selected = PIPELINE_ENTRIES
	if limit_to_ancestors_including_node != "-1":
		NODE = {"dag_node_name": limit_to_ancestors_including_node, "dag_node_deps": node_dependencies if node_dependencies else []}
		GRAPH = constructDAG(PIPELINE_ENTRIES + [NODE])  # temporary DAG including the node to get its ancestors
		ancestors = set(nx_ancestors(GRAPH, limit_to_ancestors_including_node)) | {limit_to_ancestors_including_node}
		selected = [entry for entry in selected if entry.get("dag_node_name") in ancestors]
	if not update_negative_step:
		# step -2 marks named nodes; only step -1 entries (outside the DAG) are skipped
		selected = [entry for entry in selected if entry.get("step") != -1]
	if not update_retired:
		selected = [entry for entry in selected if not entry.get("retired", False)]
	if filter_terminal_states:
		selected = [entry for entry in selected if entry.get("slurm_info", {}).get("State") not in TERMINAL_STATES]
	return selected


def refresh_pipeline_entries(entries: list) -> None:
	"""Query SLURM for the given entries and store the result in their slurm_info.

	The entries may come from any number of processing unit subdirs: all their IDs go
	into one batched query (see laupy.slurm.QUERY_CHUNK_SIZE). Entries SLURM no
	longer knows keep their stored slurm_info.
	"""
	if not entries:
		return
	# Legacy entries without slurm_id fall back to job_id
	slurm_ids = [str(entry.get("slurm_id") or entry.get("job_id")) for entry in entries]
	for entry, info in zip(entries, slurm.slurm_info(slurm_ids)):
		if info["State"] != "UNKNOWN":
			entry["slurm_info"] = info


def update_pipeline_entries(PIPELINE_ENTRIES: list, limit_to_ancestors_including_node: str = "-1", update_retired: bool = False, update_negative_step: bool = False, filter_terminal_states: bool = True, node_dependencies: list = None) -> None:
	"""Refresh slurm_info of PIPELINE_ENTRIES of one processing unit, see pipeline_entries_to_refresh() for the selection.

	To refresh many processing unit subdirs with a single query use laupy.pipeline_entries.PipelineEntriesStore.
	"""
	refresh_pipeline_entries(pipeline_entries_to_refresh(PIPELINE_ENTRIES, limit_to_ancestors_including_node, update_retired, update_negative_step, filter_terminal_states, node_dependencies))


def pipeline_entries_file(processing_unit_subdir: Union[str, Path]) -> str:
	"""Path of the file storing PIPELINE_ENTRIES of a processing unit (pipeline/dag.json)."""
	return os.path.join(str(processing_unit_subdir), "pipeline", "dag.json")


# Field names written by earlier laupy versions -> current names
_RENAMED_ENTRY_FIELDS = {"execution_unit_dir": "processing_unit_subdir", "execution_unit_name": "processing_unit_name"}


def _rename_legacy_fields(PIPELINE_ENTRIES: list) -> list:
	"""Rename fields of entries written by earlier laupy versions, in place."""
	for entry in PIPELINE_ENTRIES:
		for old, new in _RENAMED_ENTRY_FIELDS.items():
			if old in entry:
				value = entry.pop(old)
				entry.setdefault(new, value)
	return PIPELINE_ENTRIES


def load_pipeline_entries(processing_unit_subdir: Union[str, Path], topological_sort: bool = False) -> list:
	"""Load PIPELINE_ENTRIES of a processing unit; [] if there are none.

	topological_sort: order entries by the topological order of their DAG nodes.
	"""
	path = pipeline_entries_file(processing_unit_subdir)
	PIPELINE_ENTRIES = []
	if os.path.exists(path):
		with open(path) as f:
			PIPELINE_ENTRIES = _rename_legacy_fields(json.load(f))
	if topological_sort:
		PIPELINE_ENTRIES = sort_pipeline_entries_topologically(PIPELINE_ENTRIES)
	return PIPELINE_ENTRIES


def sort_pipeline_entries_topologically(PIPELINE_ENTRIES: list) -> list:
	"""Return PIPELINE_ENTRIES ordered by the topological order of their DAG nodes, then by timestamp."""
	try:
		DAG = constructDAG(PIPELINE_ENTRIES)
		# Per-node tie-breaking data:
		# - earliest timestamp for node (first timestamp wins across resubmits)
		# - first appearance position in JSON for deterministic fallback
		node_first_ts = {}
		node_first_pos = {}
		for pos, entry in enumerate(PIPELINE_ENTRIES):
			node_name = str(entry.get("dag_node_name", -1))
			ts = entry.get("timestamp", float('inf'))
			if node_name not in node_first_ts or ts < node_first_ts[node_name]:
				node_first_ts[node_name] = ts
				node_first_pos[node_name] = pos
		# Tie-breaking priority: (earliest timestamp, is integer, first position)
		tie_breaking_priority = {}
		for node in DAG:
			node_name = str(node)
			tie_breaking_priority[node_name] = (node_first_ts.get(node_name, float('inf')), string_is_integer(node_name), node_first_pos.get(node_name, float('inf')))
		ordered_nodes = list(nx_topological_sort(DAG, tie_breaking_priority=tie_breaking_priority))
		node_rank = {node: pos for pos, node in enumerate(ordered_nodes)}
		def entry_sort_key(entry):
			node_name = str(entry.get("dag_node_name", -1))
			rank = node_rank.get(node_name, -1)  # unknown nodes come first
			ts = entry.get("timestamp", float('inf'))  # missing timestamps sort last
			return (rank, ts)
		return sorted(PIPELINE_ENTRIES, key=entry_sort_key)
	except Exception as e:
		log.error(f"Error during topological sort of DAG: {e}")
		log.error(f"{e.__class__.__name__} on line {e.__traceback__.tb_lineno} in {__file__}")
		return PIPELINE_ENTRIES


def store_pipeline_entries(processing_unit_subdir: Union[str, Path], PIPELINE_ENTRIES: list) -> None:
	"""Write PIPELINE_ENTRIES of a processing unit.

	The file is replaced atomically, so concurrent readers (other CLI calls,
	lautools-browser) never see a partially written file.
	"""
	path = pipeline_entries_file(processing_unit_subdir)
	directory = os.path.dirname(path)
	os.makedirs(directory, exist_ok=True)
	handle, tmp = tempfile.mkstemp(dir=directory, prefix=".dag.json.")
	try:
		with os.fdopen(handle, "w") as f:
			json.dump(_rename_legacy_fields(PIPELINE_ENTRIES), f, indent=2)
		if os.path.exists(path):
			os.chmod(tmp, os.stat(path).st_mode & 0o7777)
		else:
			os.chmod(tmp, 0o666 & ~_current_umask())
		os.replace(tmp, path)
	except BaseException:
		if os.path.exists(tmp):
			os.unlink(tmp)
		raise


def _current_umask() -> int:
	mask = os.umask(0)
	os.umask(mask)
	return mask


def clean_pipeline_entries(processing_unit_subdir: Union[str, Path]) -> None:
	"""Forget all PIPELINE_ENTRIES of a processing unit (log files are kept)."""
	store_pipeline_entries(processing_unit_subdir, [])


# ----------------------------------------------------------------------
# Processing unit subdirs and their pipeline directory
# ----------------------------------------------------------------------

def list_processing_unit_subdirs(working_dir: Union[str, Path], pattern: str = None, require_pipeline: bool = False) -> List[Path]:
	"""Return the processing unit subdirs (immediate subdirectories) of a working directory, sorted by name.

	pattern: case-insensitive substring the directory name must contain (as `pipeline -p`).
	require_pipeline: only directories that already contain a pipeline/ directory.
	"""
	working_dir = Path(working_dir)
	subdirs = []
	for child in working_dir.iterdir():
		if not child.is_dir():  # follows symlinks
			continue
		if pattern is not None and pattern.lower() not in child.name.lower():
			continue
		if require_pipeline and not (child / "pipeline").is_dir():
			continue
		subdirs.append(child)
	return sorted(subdirs, key=lambda path: path.name.lower())


def remove_pipeline_dir(pipeline_dir: Union[str, Path]) -> None:
	"""
	Safely delete a pipeline directory and its log subdirectory.

	Rules:
	- Delete all files directly in pipeline_dir
	- Delete all files in pipeline_dir/log
	- Remove log directory
	- Remove pipeline_dir itself
	- Any other subdirectories in pipeline_dir remain untouched
	"""
	pipeline_dir = str(pipeline_dir)
	if not os.path.exists(pipeline_dir):
		print(f"No pipeline directory to delete at {pipeline_dir}")
		return
	if not os.path.isdir(pipeline_dir):
		print(f"{pipeline_dir} exists but is not a directory! Skipping deletion.")
		return
	log_dir = os.path.join(pipeline_dir, "log")
	# Delete files in pipeline_dir
	for entry in os.listdir(pipeline_dir):
		entry_path = os.path.join(pipeline_dir, entry)
		if os.path.isfile(entry_path):
			os.remove(entry_path)
	# Delete log directory and its files
	if os.path.isdir(log_dir):
		for entry in os.listdir(log_dir):
			file_path = os.path.join(log_dir, entry)
			if os.path.isfile(file_path):
				os.remove(file_path)
		os.rmdir(log_dir)
	# Finally remove pipeline_dir (should be empty except for unexpected dirs)
	try:
		os.rmdir(pipeline_dir)
	except OSError:
		print(f"Pipeline directory {pipeline_dir} not empty (other subdirs preserved).")


def string_is_integer(s: str) -> bool:
	"""Check if a string can be converted to an integer."""
	try:
		int(s)
		return True
	except ValueError:
		return False

def constructDAG(PIPELINE_ENTRIES: list) -> nx_DiGraph:
	"""
	Construct the DAG of nodes referenced by PIPELINE_ENTRIES.

	Rules:
	  - Numeric node names (>= 0) form an implicit chain:
			0 -> 1 -> 2 -> ...
		Numeric nodes may not have explicit dependencies.
	  - Named nodes use their 'dag_node_deps' list.
	  - All dependencies must refer to existing nodes, otherwise they will be ignored with a warning. 
	  - A named node with no dependencies starts a new independent branch from dag_root.
	  - The resulting graph must be acyclic.
	"""

	dag = nx_DiGraph()
	dag.add_node("dag_root")  # Add a root node for the DAG
	dag.add_node("0")  # Add the initial numeric node
	dag.add_edge("dag_root", "0")  # Connect the root to the initial numeric node

	# First add all nodes	 
	max_numeric_node = 0
	for entry in PIPELINE_ENTRIES:
		dag_node_name = entry.get("dag_node_name", None)
		if dag_node_name is None:
			dag_node_name = str(entry.get("step", 0))  # Fallback to step if dag_node_name is not provided
		dag_node_name = str(dag_node_name)	# Normalize to string for consistent handling
		entry["dag_node_name"] = dag_node_name	# Update the entry with the normalized name
		node = dag_node_name
		try:
			numeric_node = int(node)
			if numeric_node < 0:
				continue  # Skip negative numeric nodes
			max_numeric_node = max(max_numeric_node, numeric_node)
		except ValueError:
			dag.add_node(node)	# Add named nodes directly
		#Test numeric dependencies against max_numeric_node
		for dep in entry.get("dag_node_deps", []):
			try:
				numeric_dep = int(dep)
				if numeric_dep < 0:
					continue  # Skip negative numeric dependencies
				max_numeric_node = max(max_numeric_node, numeric_dep)
			except ValueError:
				pass  # Ignore non-numeric dependencies for max_numeric_node calculation

	#Make chain of existing numeric nodes
	for i in range(max_numeric_node):
		dag.add_edge(str(i), str(i + 1)) # Nodes are automatically created when adding edges, so no need to add them explicitly

	# Add dependencies
	for entry in PIPELINE_ENTRIES:
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
	if not nx_is_directed_acyclic_graph(dag):
		cycle = nx_find_cycle(dag)
		raise ValueError(f"DAG contains a cycle: {cycle}")
	return dag

def resubmit_slurm_job(PIPELINE_ENTRIES: list, entry_id: Union[str, int, Dict], env=None):
	"""Schedule the given entry again (cancelling its job if still active) and retire it.

	entry_id: the entry dict, its slurm_id (int, or str of digits) or its job_id (str).
	Returns the new entry; the caller appends it to PIPELINE_ENTRIES and saves them.
	Returns None (after logging the reason) if the entry is not found, is already retired,
	or resubmission fails.
	"""
	if isinstance(entry_id, dict):
		# Use the entry inside PIPELINE_ENTRIES (the dict passed may be a copy, e.g. from a UI)
		ENTRY = next((entry for entry in PIPELINE_ENTRIES if entry.get("job_id") == entry_id.get("job_id")), entry_id)
	elif isinstance(entry_id, (int, str)) and not isinstance(entry_id, bool):
		wanted = str(entry_id).strip()
		# Missing IDs never match (otherwise "None" would match entries without slurm_id)
		ENTRY = next((entry for entry in PIPELINE_ENTRIES
		              if (entry.get("job_id") is not None and str(entry["job_id"]) == wanted)
		              or (entry.get("slurm_id") is not None and str(entry["slurm_id"]) == wanted)), None)
	else:
		log.error(f"Cannot resubmit: entry_id must be an entry dict, a SLURM ID or a job_id, got {type(entry_id).__name__}")
		return None
	if ENTRY is None:
		log.error(f"Cannot resubmit: no pipeline entry with job_id or SLURM ID {entry_id}")
		return None
	if ENTRY.get("retired", False):
		# Resubmitting a retired entry would start a second job for the same node
		log.error(f"Cannot resubmit: entry {ENTRY.get('job_id')} (SLURM ID {ENTRY.get('slurm_id')}) for node "
		          f"'{ENTRY.get('dag_node_name', '?')}' is already retired ({ENTRY.get('retired_reason', 'no reason given')})")
		return None
	try:
		# First cancel and retire the existing job if it has a valid SLURM ID
		if "slurm_id" in ENTRY and ENTRY["slurm_id"] is not None:
			slurm_info = ENTRY.get("slurm_info", {})
			slurm_state = slurm_info.get("State", "UNKNOWN")
			if slurm_state not in TERMINAL_STATES:
				slurm.scancel(ENTRY["slurm_id"])
				log.info(f"Cancelled SLURM job with ID {ENTRY['slurm_id']} for node '{ENTRY['dag_node_name']}'")
		SLURM_CMD = ENTRY["slurm_command"].copy()  # Create a copy of the SLURM command to avoid modifying the original
		NEW_ENTRY = schedule_slurm_job(
			PIPELINE_ENTRIES=PIPELINE_ENTRIES,
			dag_node_or_step_name=ENTRY["dag_node_name"],
			script_name=ENTRY["script_name"],
			job_name=ENTRY["job_name"],
			processing_unit_subdir=ENTRY["processing_unit_subdir"],
			slurm_cmd=SLURM_CMD,
			cmd=[ENTRY["command"]],
			node_dependencies=ENTRY.get("dag_node_deps", []),
			env=env,
		)
		if NEW_ENTRY is not None:
			# The old entry is retired only once its replacement is submitted
			ENTRY["retired"] = True
			ENTRY["retire_timestamp"] = time.time()
			ENTRY["retired_reason"] = "Resubmitted"
			ENTRY["retired_by"] = NEW_ENTRY["job_id"]
		return NEW_ENTRY
	except Exception as e:
		log.error(f"Error resubmitting SLURM job for node '{ENTRY.get('dag_node_name', '?')}': {e}")
		return None

class SchedulingError(RuntimeError):
	"""A job could not be scheduled (failed dependency, sbatch failure, invalid DAG)."""


def schedule_slurm_job(PIPELINE_ENTRIES: list, dag_node_or_step_name: str, script_name: str, job_name: str, processing_unit_subdir: str, slurm_cmd: list, cmd: list, node_dependencies: list = None, env=None, raise_errors: bool = False) -> dict:
	"""Submit a SLURM job for a DAG node, depending on the active jobs of its ancestor nodes.

	Returns the new entry; the caller appends it to PIPELINE_ENTRIES and saves them.
	The new entry records the SLURM IDs it depends on in dependency_slurm_ids.
	On failure returns None after logging the error, or with raise_errors=True
	raises SchedulingError with a one-line reason instead.
	"""
	try:
		JOB_ID = uuid.uuid4().hex
		ROOTDIR = os.path.dirname(os.path.dirname(os.path.abspath(processing_unit_subdir)))
		dag_node_or_step_name = str(dag_node_or_step_name) # Normalize to string for consistent handling
		negative_node = False
		SLURM_CMD = slurm_cmd.copy() # Create a copy of the SLURM command to avoid modifying the original
		NEW_ENTRY = {}
		NEW_ENTRY["script_name"] = script_name
		NEW_ENTRY["job_name"] = job_name
		NEW_ENTRY["dag_node_name"] = dag_node_or_step_name
		NEW_ENTRY["command"] = " ".join(cmd)
		NEW_ENTRY["slurm_command"] = slurm_cmd
		NEW_ENTRY["processing_unit_name"] = os.path.basename(processing_unit_subdir)
		NEW_ENTRY["processing_unit_subdir"] = processing_unit_subdir
		if string_is_integer(dag_node_or_step_name):
			step = int(dag_node_or_step_name)
			NEW_ENTRY["step"] = step
			NEW_ENTRY["dag_node_deps"] = []  # Numeric nodes do not have explicit
			if step < 0:
				negative_node = True
		else:
			# Use -2 for named nodes, this is not triggered by includeNegativeStepNodes, which only affects step -1 nodes - submitted outside the DAG
			NEW_ENTRY["step"] = -2
			NEW_ENTRY["dag_node_deps"] = node_dependencies if node_dependencies else []
		if not negative_node:
			DAG = constructDAG(PIPELINE_ENTRIES + [NEW_ENTRY])
			#Get all ancestors
			ancestors = nx_ancestors(DAG, dag_node_or_step_name)
			dependencies = [entry for entry in PIPELINE_ENTRIES if str(entry["dag_node_name"]) in ancestors and entry.get("retired", False) == False and "slurm_id" in entry and entry.get("slurm_info", {}).get("State", "UNKNOWN") not in ["COMPLETED"] ]
			log.debug(f"Scheduling SLURM job '{job_name}' for node '{dag_node_or_step_name}' with dependencies: {[entry.get('script_name') for entry in dependencies]}")
			active_dependencies = get_active_slurmids(dependencies, raise_on_fail=True)
			NEW_ENTRY["dependency_slurm_ids"] = [int(jid) if str(jid).isdigit() else jid for jid in active_dependencies]
			if len(active_dependencies) > 0:
				SLURM_CMD.insert(1, f"--dependency=afterok:{':'.join(str(jid) for jid in active_dependencies)}")
		# For negative nodes, run the command directly without submitting to SLURM
		slurm_id = run_slurm_command(SLURM_CMD, cwd=ROOTDIR, env=env)
		if slurm_id is None:
			raise SchedulingError("sbatch failed, see the error above")
		NEW_ENTRY["job_id"] = JOB_ID
		NEW_ENTRY["slurm_id"] = slurm_id
		NEW_ENTRY["retired"] = False
		NEW_ENTRY["timestamp"] = time.time()
		return NEW_ENTRY
	except Exception as e:
		if raise_errors:
			if isinstance(e, SchedulingError):
				raise
			raise SchedulingError(str(e)) from e
		log.error(f"Error scheduling SLURM job {job_name} with step/dag_node '{dag_node_or_step_name}': {e}")
		if not isinstance(e, SchedulingError):
			log.error(f"{e.__class__.__name__} on line {e.__traceback__.tb_lineno} in {__file__}")
		return None

