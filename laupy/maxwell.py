#!/usr/bin/env python

# Query Maxwell cluster nodes and categorize them based on hardware capabilities.

import re
import csv
import os
import subprocess
from dataclasses import dataclass, field

# Node table (maxwell.tsv format): the hardware of the Maxwell nodes. Querying SLURM for the node
# features takes seconds (sinfo with %f over all nodes), querying only node states is fast, so by
# default (feature provider "app-managed-list") the hardware comes from an app-managed node table and
# SLURM is only asked which nodes exist and their states:
#   - the managed table lives at ~/.laupy/maxwell.tsv ($LAUPY_MAXWELL_TSV overrides the path);
#   - if it is missing, it is unpacked from the packaged laupy/maxwell.tsv.gz and at once updated from
#     a SLURM feature query (the packaged table fills what SLURM does not say), or, without the
#     package file, created from a SLURM feature query;
#   - if it is older than MANAGED_LIST_MAX_AGE_DAYS, it is refreshed from a SLURM feature query
#     (SLURM values win, the old table fills gaps; nodes SLURM does not report are kept);
#   - listnodes --update-default-list refreshes it on request.
# Other providers: "list" (the table as it is, no management), "slurm" (hardware from the SLURM
# features on every call, the table completes it). --list FILE / set_node_table(path) uses that file
# without managing it; --no-list / set_node_table(use_table=False) uses no table at all (SLURM only).
NODE_TABLE_ENV = "LAUPY_MAXWELL_TSV"
DEFAULT_NODE_TABLE = os.path.join("~", ".laupy", "maxwell.tsv")
PACKAGED_NODE_TABLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "maxwell.tsv.gz")
MANAGED_LIST_MAX_AGE_DAYS = 7
NODE_NOTES = []	# Messages for the user about the node table (installed, created, refreshed, refresh failed)

def default_node_table_path():
	"""Path of the app-managed node table: $LAUPY_MAXWELL_TSV or ~/.laupy/maxwell.tsv."""
	return os.path.abspath(os.path.expanduser(os.environ.get(NODE_TABLE_ENV) or DEFAULT_NODE_TABLE))

def node_table_candidates():
	"""Paths searched for the node table without an explicit one (the app-managed location)."""
	return [default_node_table_path()]

# Define hardware categories
GPU_AVAIL = ['GA100', 'RTX A6000', 'GH100', 'RTX 2080', 'H200', 'GV100GL', 'Quadro RTX', 'GP100GL', 'GB202GL RTX PRO', 'RTX6000', 'L40S']
GPU_EXCELLENT = ['GA100', 'GH100', 'H200']
GPU_STRONG = GPU_EXCELLENT + ['GV100GL', 'GB202GL RTX PRO', 'RTX6000']
GPU_WEAK = ['Quadro RTX', 'GP100GL']
GPU_ALL = list(set(GPU_AVAIL + GPU_EXCELLENT + GPU_STRONG + GPU_WEAK))
CPU_EXCELLENT = ['AMD EPYC 7F52', 'AMD EPYC 7H12', 'AMD EPYC 9534', 'AMD EPYC 9374', 'Gold 6240 CPU @ 2.60GHz',
		'Gold 6234 CPU @ 3.30GHz', 'Gold 6226 CPU @ 2.70GHz', 'AMD EPYC 9654', 'AMD EPYC 7702',
		'AMD EPYC 7643', 'AMD EPYC 7543', 'AMD EPYC 75F3', 'AMD EPYC 9554', 'AMD EPYC 9255', 'AMD EPYC 9475',
		'AMD EPYC 9575', 'Gold 6442Y'
	]
CPU_STRONG = CPU_EXCELLENT + [
		'AMD EPYC 7513', 'AMD EPYC 7642', 'AMD EPYC 7542', 'AMD EPYC 7262', 'AMD EPYC 7402',
		'Gold 6148 CPU @ 2.40GHz', 'Gold 6126 CPU @ 2.60GHz', 'Gold 6140 CPU @ 2.30GHz', 'Gold 5218 CPU @ 2.30GHz',
		'Silver 4216 CPU @ 2.10GHz'
	]
CPU_WEAK = ['E5-2698 v4 @ 2.20GHz', 'E5-2640 v4 @ 2.40GHz', 'Silver 4210 CPU @ 2.20GHz', 'Silver 4214 CPU @ 2.20GHz',
		'Silver 4114 CPU @ 2.20GHz', 'Gold 5115 CPU @ 2.40GHz', 'Gold 6230 CPU @ 2.10GHz', 'Gold 5115 CPU @ 2.40GHz',
		'Silver 4214 CPU @ 2.20GHz', 'AMD EPYC 7302'
	]
CPU_ALL = list(set(CPU_EXCELLENT + CPU_STRONG + CPU_WEAK))

def parse_maxwell_tsv(file_path):
	with open(file_path, 'r') as f:
		reader = csv.DictReader(f, delimiter='\t') # Use tab as delimiter
		rows = [row for row in reader] # Convert all rows to a list of dicts
		#GPU colums to list e.g. 0 will be [] and 2xRTX A6000 will be ['RTX A6000', 'RTX A6000']
		for row in rows:
			gpu_specs = row['GPU'].strip()
			if 'x' in gpu_specs:
				parts = gpu_specs.split('x')
				gpu_count = int(parts[0].strip())
				gpu_type = parts[1].strip()
				row['GPU'] = [gpu_type] * gpu_count
			elif gpu_specs == '0' or gpu_specs == '':
				row['GPU'] = []
			else:
				row['GPU'] = [gpu_specs]
		#partition column to list e.g. gpu hpc to ['gpu', 'hpc']
			# Tables copied from the wiki may escape underscores: ponline\_p11 -> ponline_p11
			partitions = row['Partitions'].replace('\\_', '_').strip()
			row['Partitions'] = partitions.split()
	return rows

_node_cache = None	# Rows the node classes are built from: live SLURM rows, or the table without SLURM
_table_rows = None	# Parsed node table, [] when there is none
_table_path = None	# Path of the node table in use, None when there is none
_table_override = None	# Explicit table from set_node_table()
_live_rows = None	# get_live_node_table() result once queried, None without SLURM data
_live_error = None	# Why SLURM could not be queried
_use_live = True
_use_table = True
FEATURE_PROVIDERS = ("app-managed-list", "list", "slurm")
DEFAULT_FEATURE_PROVIDER = "app-managed-list"
_feature_provider = DEFAULT_FEATURE_PROVIDER
_state_cache = {}	# Node states by partitions (table providers): one light sinfo query
ALL_NODES = []
GPU_NODES = []
EXCELLENT_NODES = []
EXCELLENT_GPU_NODES = []
EXCELLENT_CPU_NODES = []
STRONG_NODES = []
STRONG_GPU_NODES = []
STRONG_CPU_NODES = []
WEAK_NODES = []
WEAK_GPU_NODES = []
WEAK_CPU_NODES = []

def set_node_table(path=None, live=True, use_table=True, feature_provider=DEFAULT_FEATURE_PROVIDER):
	"""Choose the node sources and drop the caches.

	path: node table to use as it is (FileNotFoundError if it does not exist); None uses the
	  app-managed table (default_node_table_path()).
	live: query SLURM (sinfo); False uses the table only (offline, no refresh).
	use_table: False uses no node table at all: hardware from the SLURM features only.
	feature_provider: "app-managed-list" (default): hardware from the managed table, installed,
	  created and refreshed as needed (see the module comment), SLURM only for node states;
	  "list": hardware from the table as it is, SLURM only for node states; "slurm": hardware from
	  the SLURM node features (one slower sinfo query with features and states), the table completes
	  it. A table provider without a table falls back to "slurm".
	"""
	global _table_override, _use_live, _use_table, _node_cache, _table_rows, _table_path, _live_rows, _live_error
	global _feature_provider, _state_cache
	if path is not None and not use_table:
		raise ValueError("A node table path was given together with use_table=False")
	if feature_provider not in FEATURE_PROVIDERS:
		raise ValueError(f"feature_provider must be one of {FEATURE_PROVIDERS}, got {feature_provider!r}")
	if feature_provider == "list" and not use_table:
		raise ValueError("feature_provider='list' needs a node table, it cannot be combined with use_table=False")
	if path is not None:
		path = os.path.abspath(os.path.expanduser(path))
		if not os.path.isfile(path):
			raise FileNotFoundError(f"Node table {path} does not exist")
	_table_override = path
	_use_live = live
	_use_table = use_table
	_feature_provider = feature_provider if use_table else "slurm"
	_state_cache = {}
	_node_cache = _table_rows = _table_path = _live_rows = _live_error = None

def write_node_table(file_path, rows):
	"""Write rows as a maxwell.tsv file atomically (other processes never see half a file); returns
	the number of nodes written. Creates the directory."""
	import tempfile
	file_path = os.path.abspath(os.path.expanduser(file_path))
	os.makedirs(os.path.dirname(file_path), exist_ok=True)
	header = ["Host", "CPUCount", "CPUGFLOPS", "Memory", "GPU", "GPUFLOPS", "CPU", "Constraints", "Partitions", "Warranty"]
	handle, temporary = tempfile.mkstemp(dir=os.path.dirname(file_path), prefix=".maxwell.", suffix=".tsv")
	try:
		with os.fdopen(handle, "w") as f:
			f.write("\t".join(header) + "\n")
			for row in sorted(rows, key=lambda r: r["Host"]):
				gpus = row["GPU"]
				gpu = "+".join(f"{gpus.count(t)}x{t}" for t in sorted(set(gpus))) if gpus else "0"
				values = dict(row, GPU=gpu, Partitions=" ".join(row["Partitions"]))
				f.write("\t".join(str(values.get(k, "")) for k in header) + "\n")
		os.chmod(temporary, 0o644)
		os.replace(temporary, file_path)
	except BaseException:
		if os.path.exists(temporary):
			os.unlink(temporary)
		raise
	return len(rows)

def _combined_rows(live_rows, table_rows):
	"""Live rows (already completed from the table) plus the table nodes SLURM does not report."""
	live_hosts = {row["Host"] for row in live_rows}
	return list(live_rows) + [row for row in table_rows if row["Host"] not in live_hosts]

def update_node_table(file_path=None):
	"""Refresh a node table from a SLURM feature query and write it (default: the app-managed table).

	SLURM values win; the existing table at file_path fills what SLURM does not say and keeps the
	nodes SLURM does not report. Returns (path, number of nodes, live rows).
	Raises RuntimeError when SLURM cannot be queried.
	"""
	file_path = os.path.abspath(os.path.expanduser(file_path)) if file_path else default_node_table_path()
	old = parse_maxwell_tsv(file_path) if os.path.isfile(file_path) else []
	live_rows = get_live_node_table(old)
	count = write_node_table(file_path, _combined_rows(live_rows, old))
	return file_path, count, live_rows

def _install_packaged_node_table(file_path):
	"""Unpack laupy/maxwell.tsv.gz to file_path (atomically), keeping the archive's time stamp."""
	import gzip
	import shutil
	import tempfile
	os.makedirs(os.path.dirname(file_path), exist_ok=True)
	handle, temporary = tempfile.mkstemp(dir=os.path.dirname(file_path), prefix=".maxwell.", suffix=".tsv")
	try:
		with gzip.open(PACKAGED_NODE_TABLE, "rb") as source, os.fdopen(handle, "wb") as target:
			shutil.copyfileobj(source, target)
		os.chmod(temporary, 0o644)
		stamp = os.path.getmtime(PACKAGED_NODE_TABLE)
		os.utime(temporary, (stamp, stamp))
		os.replace(temporary, file_path)
	except BaseException:
		if os.path.exists(temporary):
			os.unlink(temporary)
		raise

def _prepare_managed_node_table():
	"""Make sure the app-managed table exists and is recent; may set _live_rows (when it queried SLURM).

	Returns the path, or None when there is no table and none could be made."""
	global _live_rows, _live_error
	import time
	path = default_node_table_path()
	refresh_now = False
	if not os.path.isfile(path):
		if os.path.isfile(PACKAGED_NODE_TABLE):
			# The packaged table is the starting point (FLOPS, warranty, nodes without features);
			# it is updated from SLURM right away below
			_install_packaged_node_table(path)
			NODE_NOTES.append(f"Installed the node table {path} from the laupy package")
			refresh_now = True
		elif _use_live:
			try:
				path, count, _live_rows = update_node_table(path)
				NODE_NOTES.append(f"Created the node table {path} from SLURM ({count} nodes)")
				return path
			except (RuntimeError, OSError) as e:
				_live_error = str(e)
				return None
		else:
			return None
	age_days = (time.time() - os.path.getmtime(path)) / 86400
	if _use_live and (refresh_now or age_days > MANAGED_LIST_MAX_AGE_DAYS):
		reason = "first use" if refresh_now else f"was {age_days:.0f} days old"
		try:
			path, count, _live_rows = update_node_table(path)
			NODE_NOTES.append(f"Updated the node table {path} from SLURM ({count} nodes, {reason})")
		except (RuntimeError, OSError) as e:
			NODE_NOTES.append(f"Could not update the node table {path} from SLURM ({reason}), using it as it is: {e}")
			if refresh_now:	# Mark the unpacked table as outdated, so the next call tries again
				stale = time.time() - (MANAGED_LIST_MAX_AGE_DAYS + 1) * 86400
				os.utime(path, (stale, stale))
	return path

def find_node_table():
	"""Path of the node table to use, or None when there is none or tables are ignored (not an error).

	With the app-managed provider this installs, creates or refreshes the managed table."""
	if not _use_table:
		return None
	if _table_override:
		return _table_override
	if _feature_provider == "app-managed-list":
		return _prepare_managed_node_table()
	path = default_node_table_path()
	return path if os.path.isfile(path) else None

def load_node_table():
	"""The parsed node table ([] without one), cached."""
	global _table_rows, _table_path
	if _table_rows is None:
		_table_path = find_node_table()
		_table_rows = parse_maxwell_tsv(_table_path) if _table_path else []
	return _table_rows

def get_node_source():
	"""Where the node classes come from: {"source": "sinfo" | table path | None, "table": path or None,
	"live_error": str or None, "feature_provider": the provider asked for, "hardware_from": "slurm" |
	"list" | None (what was actually used)}."""
	init_node_cache()
	source = "sinfo" if _live_rows is not None else _table_path
	return {"source": source, "table": _table_path, "live_error": _live_error, "feature_provider": _feature_provider,
	        "hardware_from": "slurm" if _live_rows is not None else ("list" if _table_path else None)}

def init_node_cache():
	"""Build the node lists: from SLURM (one sinfo query) with the table filling gaps, or from the
	table alone when SLURM is not used or not available. Without both the lists stay empty."""
	global _node_cache, _live_rows, _live_error
	global ALL_NODES
	global GPU_NODES
	global EXCELLENT_NODES
	global EXCELLENT_GPU_NODES
	global EXCELLENT_CPU_NODES
	global STRONG_NODES
	global STRONG_GPU_NODES
	global STRONG_CPU_NODES
	global WEAK_NODES
	global WEAK_GPU_NODES
	global WEAK_CPU_NODES
	if _node_cache is None:
		table = load_node_table()	# May install, create or refresh the app-managed table (and set _live_rows)
		if _live_rows is None and _live_error is None and _use_live and (_feature_provider == "slurm" or not table):
			try:
				_live_rows = get_live_node_table(table)
			except RuntimeError as e:
				_live_error = str(e)
		_node_cache = _live_rows if _live_rows is not None else table
		ALL_NODES = []
		GPU_NODES = []
		EXCELLENT_NODES = []
		EXCELLENT_GPU_NODES = []
		EXCELLENT_CPU_NODES = []
		STRONG_NODES = []
		STRONG_GPU_NODES = []
		STRONG_CPU_NODES = []
		WEAK_NODES = []
		WEAK_GPU_NODES = []
		WEAK_CPU_NODES = []
		for node in _node_cache:
			node_name = node['Host']
			node_gpus = node['GPU']
			cpu_type = node['CPU']
			ALL_NODES.append(node_name)
			if any(gpu in GPU_EXCELLENT for gpu in node_gpus):
				EXCELLENT_GPU_NODES.append(node_name)
				EXCELLENT_NODES.append(node_name)
			if any(gpu in GPU_STRONG for gpu in node_gpus):
				STRONG_GPU_NODES.append(node_name)
				STRONG_NODES.append(node_name)
			if any(gpu in GPU_WEAK for gpu in node_gpus):
				WEAK_GPU_NODES.append(node_name)
				WEAK_NODES.append(node_name)
			if len(node_gpus) == 0:
				if cpu_type in CPU_EXCELLENT:
					EXCELLENT_NODES.append(node_name)
				elif cpu_type in CPU_STRONG:
					STRONG_NODES.append(node_name)
				elif cpu_type in CPU_WEAK:
					WEAK_NODES.append(node_name)
			else:
				GPU_NODES.append(node_name)
			if cpu_type in CPU_EXCELLENT:
				EXCELLENT_CPU_NODES.append(node_name)
			if cpu_type in CPU_STRONG:
				STRONG_CPU_NODES.append(node_name)
			if cpu_type in CPU_WEAK:
				WEAK_CPU_NODES.append(node_name)

def get_node_description():
	init_node_cache()
	return _node_cache

def partition_list(partition):
	"""Normalize a partition argument to a list of names.

	Accepts "allcpu,allgpu", ["allcpu", "allgpu"] (or a tuple/set) or None.
	Names are stripped and empty ones dropped; None or "" gives [] (all partitions).
	"""
	if partition is None:
		return []
	if isinstance(partition, str):
		partitions = partition.split(',')
	elif isinstance(partition, (list, tuple, set)):
		partitions = [str(p) for p in partition]
	else:
		raise ValueError(f"Invalid partition type: {type(partition)}")
	return [p.strip() for p in partitions if p.strip()]

def get_all_nodes(partition=None):
	init_node_cache()
	if partition is not None:
		partitions = partition_list(partition)
		if not partitions:
			return ALL_NODES
		# Collect nodes that belong to any of the specified partitions
		partitioned_nodes = [ node['Host'] for node in _node_cache if any(part in node['Partitions'] for part in partitions)]
		return partitioned_nodes
	else:
		return ALL_NODES

def get_gpu_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in GPU_NODES if n in NODES_PARTITION] 
	return NODES

def get_excellent_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in EXCELLENT_NODES if n in NODES_PARTITION] 
	return NODES

def get_strong_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in STRONG_NODES if n in NODES_PARTITION] 
	return NODES

def get_weak_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in WEAK_NODES if n in NODES_PARTITION] 
	return NODES

def get_excellent_gpu_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in EXCELLENT_GPU_NODES if n in NODES_PARTITION] 
	return NODES

def get_strong_gpu_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in STRONG_GPU_NODES if n in NODES_PARTITION] 
	return NODES

def get_weak_gpu_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in WEAK_GPU_NODES if n in NODES_PARTITION] 
	return NODES

def get_excellent_cpu_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in EXCELLENT_CPU_NODES if n in NODES_PARTITION] 
	return NODES

def get_strong_cpu_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in STRONG_CPU_NODES if n in NODES_PARTITION] 
	return NODES

def get_weak_cpu_nodes(partition=None):
	init_node_cache()
	NODES_PARTITION = get_all_nodes(partition)
	NODES = [n for n in WEAK_CPU_NODES if n in NODES_PARTITION] 
	return NODES

# Handling lists of nodes and sinfo output
# Function to run a shell command and get the output
def run_command(command):
	result = subprocess.run(command, capture_output=True, text=True)
	if result.returncode != 0:
		print(f"Error: {result.stderr}")
		exit(1)
	return result.stdout

# Expand node ranges in the format "node[001-005]"
def expand_node_ranges(entry):
	nodes = []
	if '[' in entry:  # If there are ranges
		prefix = entry.split('[')[0]
		ranges = entry.split('[')[1].split(']')[0]
		ranges_list = ranges.split(',')
		for part in ranges_list:
			if '-' in part:  # Range
				start, end = map(int, part.split('-'))
				for i in range(start, end + 1):
					nodes.append(f"{prefix}{i:03d}")
			else:
				nodes.append(f"{prefix}{int(part):03d}")
	else:
		nodes.append(entry)
	return nodes

# Convert node string to list of nodes, expanding ranges
def nodes_from_string(nodesStr):
	# Regular expression to match the part inside square brackets and prevent premature splitting
	node_entries = re.findall(r'[^\[\],]+(?:\[[^\]]+\])?', nodesStr)
	node_array = []
	for entry in node_entries:
		expanded = expand_node_ranges(entry)
		node_array.extend(expanded)
	return node_array

# Get list of nodes in the partition (using sinfo)
def get_live_nodes(partition = None, idle_only = False):
	"""Nodes SLURM reports in the partitions (any state), optionally only idle ones.

	Uses the same sinfo query as the node classes, so submitslurm and listnodes need one query;
	falls back to a plain sinfo call when that query is switched off or failed.
	"""
	if _use_live:
		init_node_cache()
	if _live_rows is None and _use_live and _feature_provider != "slurm":
		states = _light_node_states(partition)	# Table providers: only node states are queried
		return [host for host, state in states.items() if not idle_only or state.startswith("idle")]
	if _live_rows is not None:
		partitions = partition_list(partition)
		return [row["Host"] for row in _live_rows
		        if (not partitions or any(p in row["Partitions"] for p in partitions))
		        and (not idle_only or row["state"].startswith("idle"))]
	sinfo_cmd = ["sinfo"]
	partitions = partition_list(partition)
	if partitions:
		sinfo_cmd = ["sinfo", "-p", ",".join(partitions)]
	sinfo_output = run_command(sinfo_cmd)
	nodes = []
	for line in sinfo_output.splitlines()[1:]:	# Skip header
		if not idle_only or "idle" in line:
			nodes.append(line.split()[5])  # Assuming the node list is in the 6th column
	return nodes_from_string(",".join(nodes))

# Node selection for listnodes and submitslurm: one cached sinfo query for classes and states

NODE_STATES = ("any", "up", "idle")

def _light_node_states(partition=None):
	"""{host: state} of the partitions from the light sinfo query (feature provider "list"), cached."""
	key = tuple(partition_list(partition))
	if key not in _state_cache:
		_state_cache[key] = get_live_node_states(list(key))
	return _state_cache[key]

def _node_states(partition=None):
	"""{host: state} from the cached live query, or None without live data."""
	init_node_cache()
	if _live_rows is not None:
		return {row["Host"]: row["state"] for row in _live_rows}
	if _use_live and _feature_provider != "slurm":
		return _light_node_states(partition)
	return None

def select_nodes(partition=None, node_class=None, gpu_only=False, nodelist=None, state="any"):
	"""Nodes of the partitions that SLURM reports, filtered; sorted host names.

	node_class: None (all), "excellent", "strong" (includes excellent) or "weak" (see init_node_cache()).
	gpu_only: only nodes with GPUs. nodelist: only these nodes (SLURM syntax "max-wn[001-004],max-a").
	state: "any" (every node SLURM reports), "up" (can run jobs: not down, drained or not responding)
	  or "idle".
	Uses the cached sinfo query of the node lists; only without it (SLURM switched off or failed)
	get_live_nodes() makes a plain sinfo call, and "up" then counts every listed node.
	"""
	if state not in NODE_STATES:
		raise ValueError(f"state must be one of {NODE_STATES}, got {state!r}")
	init_node_cache()
	candidates = set(get_live_nodes(partition, idle_only=(state == "idle")))
	states = _node_states(partition)
	if state == "up" and states is not None:
		candidates = {n for n in candidates if node_is_up(states.get(n))}
	if node_class is not None:
		members = {"excellent": EXCELLENT_NODES, "strong": STRONG_NODES, "weak": WEAK_NODES}.get(node_class)
		if members is None:
			raise ValueError(f"node_class must be excellent, strong or weak, got {node_class!r}")
		candidates &= set(members)
	if gpu_only:
		candidates &= set(GPU_NODES)
	if nodelist:
		candidates &= set(nodes_from_string(nodelist) if isinstance(nodelist, str) else nodelist)
	return sorted(candidates)

@dataclass
class NodeRequest:
	"""What submitslurm passes to sbatch for a node selection (see sbatch_node_request())."""
	nodelist: list = field(default_factory=list)	# --nodelist, [] for none
	exclude: list = field(default_factory=list)	# --exclude, [] for none
	error: str = None	# Set when nothing can be submitted (explicit nodes or GPUs not available)
	notes: list = field(default_factory=list)	# Information for the user (fallbacks, skipped nodes)

	def sbatch_arguments(self):
		"""--nodelist=/--exclude= arguments; never empty lists (sbatch rejects them)."""
		arguments = []
		if self.nodelist:
			arguments.append(f"--nodelist={','.join(self.nodelist)}")
		if self.exclude:
			arguments.append(f"--exclude={','.join(self.exclude)}")
		return arguments

def sbatch_node_request(partition=None, nodelist=None, gpu_only=False, excellent=False, strong=False, exclude_weak=False):
	"""Node options of submitslurm (-n, -g, -e, -s, -x) as sbatch arguments, from one cached sinfo query.

	-n (nodelist) takes precedence over -e over -s; -g limits any of them to GPU nodes, -x then removes
	weak nodes from the required ones, or alone gives --exclude with the weak nodes.
	Required nodes (--nodelist) are limited to nodes that are up in the partitions: a node that is down
	or drained in the list would keep the job pending. Weak nodes to exclude are those SLURM reports.
	An explicit node list or a GPU request that no available node matches gives error; an excellent
	or strong request without available nodes falls back to no restriction (with a note).
	"""
	request = NodeRequest()
	node_class = None if nodelist else ("excellent" if excellent else ("strong" if strong else None))
	if nodelist or gpu_only or node_class:
		request.nodelist = select_nodes(partition, node_class=node_class, gpu_only=gpu_only,
		                                nodelist=nodelist if nodelist else None, state="up")
		if exclude_weak:	# Combined with -x: leave the weak nodes out of the required ones
			request.nodelist = sorted(set(request.nodelist) - set(WEAK_NODES))
		if nodelist:
			unavailable = sorted(set(nodes_from_string(nodelist)) - set(request.nodelist))
			if unavailable and request.nodelist:
				request.notes.append(f"Skipping {len(unavailable)} requested nodes that are not up in {partition}: "
				                     f"{compress_node_list(unavailable)}")
		if not request.nodelist:
			if nodelist or gpu_only:
				what = "--nodelist" if nodelist else "--gpu-nodes"
				request.error = f"No available nodes match {what} in partition {partition}"
			else:
				request.notes.append(f"No available {node_class} nodes in partition {partition}, "
				                     "submitting without node restrictions")
	elif exclude_weak:
		request.exclude = select_nodes(partition, node_class="weak", state="any")
	return request

# Cluster topology: the hardware description from maxwell.tsv combined with live sinfo states

# Compact sinfo states (%t) of nodes that accept or run jobs; a trailing "*" means not responding
UP_STATES = ("idle", "mix", "alloc", "comp", "resv", "plnd")

def compress_node_list(nodes):
	"""Inverse of nodes_from_string: ["max-wng004", "max-wng005", "max-a1"] -> "max-a1,max-wng[004-005]"."""
	groups = {}
	plain = []
	for node in sorted(set(nodes)):
		match = re.match(r"^(.*?)(\d{3})$", node)
		if match:
			groups.setdefault(match.group(1), []).append(int(match.group(2)))
		else:
			plain.append(node)
	parts = list(plain)
	for prefix, numbers in groups.items():
		ranges = []
		start = previous = numbers[0]
		for number in numbers[1:] + [None]:
			if number is not None and number == previous + 1:
				previous = number
				continue
			ranges.append(f"{start:03d}" if start == previous else f"{start:03d}-{previous:03d}")
			if number is not None:
				start = previous = number
		if len(ranges) == 1 and "-" not in ranges[0]:
			parts.append(f"{prefix}{ranges[0]}")
		else:
			parts.append(f"{prefix}[{','.join(ranges)}]")
	return ",".join(sorted(parts))

def get_live_node_states(partition=None):
	"""Query sinfo for the state of each node: {"max-wng004": "idle", ...}.

	States are the compact sinfo states (%t), e.g. idle, mix, alloc, drain, down*.
	Raises RuntimeError when sinfo is not available or fails.
	"""
	sinfo_cmd = ["sinfo", "-N", "-h", "-o", "%N %t"]
	partitions = partition_list(partition)
	if partitions:
		sinfo_cmd += ["-p", ",".join(partitions)]
	states = {}
	for line in _run_slurm_query(sinfo_cmd, "node states").splitlines():
		fields = line.split()
		if len(fields) >= 2:
			states.setdefault(fields[0], fields[1])	# A node in several partitions is listed once per partition
	return states

def node_is_up(state):
	"""True for a compact sinfo state of a node that can run jobs (not down, drained or not responding)."""
	return state is not None and not state.endswith("*") and state.rstrip("~#!%$@^-") in UP_STATES

NODE_CLASSES = ("excellent", "strong", "weak", "default", "unclassified")

def unknown_hardware(node):
	"""Hardware of a maxwell.tsv row that is in no list of this module: ["GPU X", ...] or ["CPU Y"].

	A node with GPUs is judged by its GPUs (known: GPU_ALL), a node without GPU by its CPU (known: CPU_ALL).
	"""
	if node["GPU"]:
		return [f"GPU {t}" for t in sorted(set(node["GPU"])) if t not in GPU_ALL]
	if node["CPU"] not in CPU_ALL:
		return [f"CPU {node['CPU']}"]
	return []

def node_class(node):
	"""Class of a node (maxwell.tsv or live row) from its hardware, as used by submitslurm -e/-s/-x.

	excellent, strong or weak from the hardware lists (a node with GPUs by its best GPU, a node
	without GPU by its CPU); default when the hardware is in GPU_ALL/CPU_ALL but in none of these
	classes; unclassified when it is not in GPU_ALL/CPU_ALL.
	"""
	if node["GPU"]:
		lists = (("excellent", GPU_EXCELLENT), ("strong", GPU_STRONG), ("weak", GPU_WEAK))
		for name, models in lists:
			if any(gpu in models for gpu in node["GPU"]):
				return name
	else:
		for name, models in (("excellent", CPU_EXCELLENT), ("strong", CPU_STRONG), ("weak", CPU_WEAK)):
			if node["CPU"] in models:
				return name
	return "unclassified" if unknown_hardware(node) else "default"

# Live node table from SLURM: sinfo is the source of truth, maxwell.tsv only fills gaps

# GPU model features of the Maxwell nodes -> GPU names of maxwell.tsv used in the GPU_* lists.
# A feature that is not here is used as it is (e.g. L40S) and shows up as unclassified.
GPU_FEATURE_NAMES = {
	"A100": "GA100", "H100": "GH100", "H200": "H200", "V100": "GV100GL", "P100": "GP100GL",
	"A6000": "RTX A6000", "RTX8000": "Quadro RTX",
}
# Features used for different GPU models: the model is taken from maxwell.tsv when the node is there
GPU_AMBIGUOUS_FEATURES = {"RTX6000"}
# Features that say nothing about the hardware model
_GENERIC_FEATURES = {"GPU", "BS", "EPYC", "INTEL", "V4", "AC", "AP"}

SINFO_NODE_FORMAT = "%N|%P|%t|%c|%z|%m|%G|%f|%C"

def _known_cpu_name(name, known):
	"""'Gold 6240' -> 'Gold 6240 CPU @ 2.60GHz' when such a name is known; otherwise name itself."""
	for candidate in sorted(known):
		if candidate == name or candidate.startswith(name + " "):
			return candidate
	return name

def hardware_from_features(features, table_row=None, known_cpus=None):
	"""CPU and GPU model from SLURM node features such as "EPYC, 7543, 512G, GPU, GPUx1, A100, A100-PCIE-40GB".

	Returns (cpu, gpu_model, gpu_count_feature): names as used in maxwell.tsv and the GPU_*/CPU_*
	lists where they can be matched, None where the features do not say. table_row (the node in
	maxwell.tsv) resolves ambiguous GPU features. known_cpus: CPU names to match (default CPU_ALL and
	the CPUs of the node table if it is loaded).
	"""
	if known_cpus is None:
		known_cpus = set(CPU_ALL) | {row["CPU"] for row in (_table_rows or [])}
	tokens = [t.strip() for t in features.split(",") if t.strip() and t.strip() != "(null)"]
	cpu = None
	for i, token in enumerate(tokens):
		match = re.match(r"^(Gold|Silver|Platinum|Bronze)-(\w+)$", token)
		if match:
			cpu = _known_cpu_name(f"{match.group(1)} {match.group(2)}", known_cpus)
		elif re.match(r"^E[57]-\d{4}$", token):
			cpu = _known_cpu_name(f"{token} v4" if "V4" in tokens else token, known_cpus)
		elif token == "EPYC" and i + 1 < len(tokens):
			model = tokens[i + 1]
			name = f"AMD EPYC {model}"
			if name not in known_cpus and re.match(r"^\d{4}F$", model):
				name = f"AMD EPYC {model[:-1]}"	# 9374F is listed as AMD EPYC 9374
			cpu = _known_cpu_name(name, known_cpus)
	gpu_count = None
	gpu_model = None
	if "GPU" in tokens:
		for token in tokens:
			match = re.match(r"^GPUx(\d+)$", token)
			if match:
				gpu_count = int(match.group(1))
		models = [t for t in tokens if t not in _GENERIC_FEATURES and "-" not in t
		          and not re.match(r"^(GPUx\d+|\d+G|\d\w{3}F?)$", t)]
		if models:
			gpu_model = models[0]
			if gpu_model in GPU_FEATURE_NAMES:
				gpu_model = GPU_FEATURE_NAMES[gpu_model]
			elif gpu_model in GPU_AMBIGUOUS_FEATURES and table_row and table_row["GPU"]:
				gpu_model = table_row["GPU"][0]	# RTX6000: RTX A6000 or GB202GL RTX PRO
	return cpu, gpu_model, gpu_count

def gres_gpu_count(gres):
	"""GPUs in a sinfo %G string: "gpu:4", "gpu:a100:4(S:0-1)", "gpu:2,shard:8" -> 4, 4, 2; None without GPUs."""
	count = None
	for item in re.split(r",(?![^()]*\))", gres or ""):
		item = re.sub(r"\(.*\)$", "", item.strip())
		fields = item.split(":")
		if fields[0] == "gpu" and len(fields) >= 2 and fields[-1].isdigit():
			count = (count or 0) + int(fields[-1])
	return count

def get_live_node_table(table=None):
	"""All nodes as SLURM reports them (sinfo -a -N), in the row format of parse_maxwell_tsv().

	Partitions, state, CPUs, memory, GPU count (gres) and the CPU/GPU model (node features) come
	from SLURM. Only what SLURM does not say is taken from the table (default: maxwell.tsv): the
	FLOPS and Warranty columns, and the hardware of nodes without features. Each row also has
	"state", "cpu_load" ({"alloc", "idle", "other", "total"} CPUs or None) and "source" (fields
	filled from the table, [] if none).
	Raises RuntimeError when sinfo is not available or fails.
	"""
	if table is None:
		table = load_node_table()
	by_host = {row["Host"]: row for row in table}
	known_cpus = set(CPU_ALL) | {row["CPU"] for row in table}	# Once per query, not per node
	rows = {}
	output = _run_slurm_query(["sinfo", "-a", "-N", "-h", "-o", SINFO_NODE_FORMAT], "node features and states")
	for line in output.splitlines():
		fields = line.split("|")
		if len(fields) < 8:
			continue
		host, partition, state, cpus, sct, memory, gres, features = (f.strip() for f in fields[:8])
		cpu_load = None	# %C: allocated/idle/other/total CPUs
		if len(fields) > 8 and fields[8].count("/") == 3:
			try:
				alloc, idle, other, total = (int(x) for x in fields[8].strip().split("/"))
				cpu_load = {"alloc": alloc, "idle": idle, "other": other, "total": total}
			except ValueError:
				pass
		partition = partition.rstrip("*")	# The default partition is marked with *
		if host in rows:
			if partition not in rows[host]["Partitions"]:
				rows[host]["Partitions"].append(partition)
			continue
		known = by_host.get(host)
		cpu, gpu_model, gpu_feature_count = hardware_from_features(features, known, known_cpus)
		gpu_count = gres_gpu_count(gres)
		if gpu_count is None:
			gpu_count = gpu_feature_count
		source = []
		if cpu is None:
			cpu = known["CPU"] if known else "unknown CPU"
			source.append("CPU")
		if gpu_model is None and (gpu_count or (gpu_count is None and known and known["GPU"])):
			gpu_model = known["GPU"][0] if known and known["GPU"] else "unknown GPU"
			source.append("GPU")
		if gpu_count is None:
			gpu_count = len(known["GPU"]) if known else 0
			if gpu_count:
				source.append("GPU count")
		sockets_cores_threads = sct.replace(":", "/")
		cpu_count = f"{cpus} ({sockets_cores_threads})" if cpus.isdigit() and sct.count(":") == 2 else cpus
		memory_gb = f"{round(int(memory) / 1024)}GB" if memory.isdigit() else (known["Memory"] if known else "")
		rows[host] = {
			"Host": host,
			"CPUCount": cpu_count,
			"CPUGFLOPS": known.get("CPUGFLOPS", "") if known else "",
			"Memory": memory_gb,
			"GPU": [gpu_model] * gpu_count if gpu_model and gpu_count else [],
			"GPUFLOPS": known.get("GPUFLOPS", "") if known else "",
			"CPU": cpu,
			"Constraints": ", ".join(t.strip() for t in features.split(",") if t.strip() and t.strip() != "(null)"),
			"Partitions": [partition],
			"Warranty": known.get("Warranty", "") if known else "",
			"state": state,
			"cpu_load": cpu_load,
			"source": source,
		}
	return list(rows.values())

def export_topology(file_path, rows=None):
	"""Write the node table as a maxwell.tsv file; returns the number of nodes written.

	Default rows: the nodes SLURM reports, combined with the node table in use (SLURM values win, the
	table fills FLOPS, warranty, hardware of nodes without features and ambiguous GPUs). Table nodes
	that SLURM does not report (e.g. display nodes) are kept as they are in the table, to be curated
	by hand. Reuses the cached feature query if there is one. Raises RuntimeError when SLURM cannot
	be queried.
	"""
	if rows is None:
		init_node_cache()
		live_rows = _live_rows
		if live_rows is None:
			if not _use_live:
				raise RuntimeError("SLURM queries are switched off (set_node_table(live=False))")
			live_rows = get_live_node_table(load_node_table())
		rows = _combined_rows(live_rows, load_node_table())
	return write_node_table(file_path, rows)

def _hardware_text(row):
	gpus = row["GPU"]
	gpu = "+".join(f"{gpus.count(t)}x{t}" for t in sorted(set(gpus))) if gpus else "no GPU"
	return f"{row['CPU']}, {gpu}"

def get_cluster_topology(partition=None, live=True, table_path=None):
	"""Describe the cluster: nodes, hardware, classes and (live) states.

	partition: limit to these partitions (str "allcpu,allgpu", list, or None for all).
	Sources as chosen by set_node_table(): SLURM (the same sinfo query as the node lists) is the source
	of truth; the node table, if there is one (find_node_table()), fills what sinfo does not say,
	resolves ambiguous features and is compared with SLURM. If sinfo fails, the table is used without
	states and live_error is set; without a table the topology is then empty.
	live: False queries nothing and describes the table (offline).
	table_path: shortcut for set_node_table(table_path, live=live) before describing.
	Returns a dict:
	  source           "sinfo", the path of the table used for the hardware, or None (neither available)
	  table            path of the node table, None without one (see find_node_table())
	  partitions       the partitions asked for ([] = all)
	  live_error       None, or why sinfo could not be queried (live states missing)
	  nodes            [{"Host", "CPU", "GPU" (list), "Memory", "CPUCount", "Partitions", "Warranty",
	                     "class", "state" (None without live data), "up" (bool or None)}]
	  gpu_types        {gpu_type: {"nodes": [...], "gpus": count}} for nodes with GPUs
	  cpu_types        {cpu_type: [nodes]} for nodes without GPUs
	  classes          {"excellent"|"strong"|"weak"|"default"|"unclassified": [nodes]}, see node_class()
	  unknown_hardware {"GPU X"|"CPU Y": [nodes]} hardware not in GPU_ALL/CPU_ALL (the unclassified nodes)
	  not_described    nodes reported by sinfo but missing in the table
	  not_in_sinfo     table nodes in the partitions that sinfo does not report (sinfo source only)
	  hardware_differs [(host, table hardware, sinfo hardware)] (sinfo source only)
	  filled_from_table {host: [fields]} where sinfo said nothing and maxwell.tsv was used (sinfo source only)
	"""
	if table_path is not None:
		set_node_table(table_path, live=live)
	partitions = partition_list(partition)
	live_error = None
	live_rows = None
	states = {}
	table = load_node_table()
	table_file = _table_path
	if live:
		if _use_live:
			init_node_cache()	# The one sinfo query, shared with get_*_nodes()
			live_rows, live_error = _live_rows, _live_error
			if live_rows is None and live_error is None:	# Feature provider "list": light state query
				try:
					states = _light_node_states(partitions)
				except RuntimeError as e:
					live_error = str(e)
		else:
			live_error = "SLURM queries are switched off (set_node_table(live=False))"
		if live_rows is not None:
			states = {row["Host"]: row["state"] for row in live_rows}
	source = "sinfo" if live_rows is not None else table_file
	use_states = live and live_error is None
	rows = live_rows if live_rows is not None else table
	nodes = []
	for row in rows:
		if partitions and not any(p in row["Partitions"] for p in partitions):
			continue
		state = states.get(row["Host"]) if use_states else None
		nodes.append({
			"Host": row["Host"],
			"CPU": row.get("CPU", ""),
			"GPU": list(row["GPU"]),
			"Memory": row.get("Memory", ""),
			"CPUCount": row.get("CPUCount", ""),
			"Partitions": list(row["Partitions"]),
			"Warranty": row.get("Warranty", ""),
			"class": node_class(row),
			"state": state,
			"up": node_is_up(state) if use_states else None,
		})
	gpu_types = {}
	cpu_types = {}
	classes = {name: [] for name in NODE_CLASSES}
	unknown = {}
	for node in nodes:
		classes[node["class"]].append(node["Host"])
		for hardware in unknown_hardware(node):
			unknown.setdefault(hardware, []).append(node["Host"])
		if node["GPU"]:
			for gpu_type in sorted(set(node["GPU"])):
				info = gpu_types.setdefault(gpu_type, {"nodes": [], "gpus": 0})
				info["nodes"].append(node["Host"])
				info["gpus"] += node["GPU"].count(gpu_type)
		else:
			cpu_types.setdefault(node["CPU"], []).append(node["Host"])
	described = {row["Host"]: row for row in table}
	shown = {node["Host"] for node in nodes}
	not_in_sinfo, hardware_differs, filled = [], [], {}
	if live_rows is not None and table:
		live_hosts = {row["Host"] for row in live_rows}
		not_in_sinfo = sorted(row["Host"] for row in table if row["Host"] not in live_hosts
		                      and (not partitions or any(p in row["Partitions"] for p in partitions)))
		for row in live_rows:
			if row["Host"] not in shown:
				continue
			if row["source"]:
				filled[row["Host"]] = row["source"]
			old = described.get(row["Host"])
			if old and _hardware_text(old) != _hardware_text(row):
				hardware_differs.append((row["Host"], _hardware_text(old), _hardware_text(row)))
	return {
		"source": source,
		"partitions": partitions,
		"live_error": live_error,
		"nodes": nodes,
		"gpu_types": gpu_types,
		"cpu_types": cpu_types,
		"classes": classes,
		"unknown_hardware": unknown,
		"table": table_file,
		"not_described": sorted(n for n in states if table and n not in described and (live_rows is None or n in shown)),
		"not_in_sinfo": not_in_sinfo,
		"hardware_differs": hardware_differs,
		"filled_from_table": filled,
	}

def format_cluster_topology(topology, verbose=False, user=None, color=False):
	"""Human readable report of get_cluster_topology(); verbose adds one line per node.

	For a workload report, user (default: the current user) marks own jobs, in green with color=True.
	"""
	if user is None:
		user = current_user()
	nodes_by_host = {node["Host"]: node for node in topology["nodes"]}
	live = topology["live_error"] is None and any(node["state"] is not None for node in topology["nodes"])

	workload = topology.get("workload")	# Set by get_cluster_workload(): adds the Idle column and users

	def count(hosts):
		if not live:
			return f"{len(hosts):5d}"
		up = sum(1 for h in hosts if nodes_by_host[h]["up"])
		if workload:
			idle = sum(1 for h in hosts if nodes_by_host[h].get("idle"))
			return f"{len(hosts):5d} {up:5d} {idle:5d}"
		return f"{len(hosts):5d} {up:5d}"

	header = "Nodes" if not live else ("Nodes    Up  Idle" if workload else "Nodes    Up")
	lines = []
	where = ",".join(topology["partitions"]) if topology["partitions"] else "all partitions"
	lines.append(f"Maxwell cluster topology ({where})")
	from_sinfo = topology.get("source") == "sinfo"
	table = topology.get("table")
	table_name = os.path.basename(table) if table else "the node table"
	if topology["live_error"]:
		lines.append(f"Live node states unavailable: {topology['live_error']}")
	elif not live and not from_sinfo:
		lines.append("Live node states not queried.")
	if from_sinfo:
		lines.append(f"Hardware from SLURM (sinfo node features): {len(topology['nodes'])} nodes")
		if table:
			lines.append(f"Node table: {table}")
		elif not _use_table:
			lines.append("Node table: ignored (live data only)")
		else:
			lines.append("Node table: none found (" + ", ".join(node_table_candidates()) + "), SLURM only")
	elif topology.get("source"):
		lines.append(f"Hardware from {topology['source']}: {len(topology['nodes'])} nodes")
	else:
		slurm = "SLURM is not available" if topology["live_error"] else "SLURM was not queried"
		lines.append(f"No node data: {slurm} and no node table was found ("
		             + ", ".join(node_table_candidates()) + ").")
	if live:
		states = {}
		for node in topology["nodes"]:
			key = node["state"] if node["state"] is not None else "not in sinfo"
			states[key] = states.get(key, 0) + 1
		up = sum(1 for node in topology["nodes"] if node["up"])
		lines.append(f"Up: {up}; states: " + ", ".join(f"{k} {v}" for k, v in sorted(states.items(), key=lambda kv: -kv[1])))
	lines.append("")
	if topology["gpu_types"]:
		lines.append("GPU nodes by GPU type (and by GPUs per node)")
		lines.append(f"  {'GPU type':<22} {header}  GPUs  Nodes")
		for gpu_type, info in sorted(topology["gpu_types"].items(), key=lambda kv: -len(kv[1]["nodes"])):
			per_node = {}
			for host in info["nodes"]:
				per_node.setdefault(nodes_by_host[host]["GPU"].count(gpu_type), []).append(host)
			if len(per_node) == 1:
				(n, hosts), = per_node.items()
				label = f"{gpu_type} ({n}/node)"
				lines.append(f"  {label:<22} {count(hosts)} {info['gpus']:5d}  {compress_node_list(hosts)}")
				continue
			lines.append(f"  {gpu_type:<22} {count(info['nodes'])} {info['gpus']:5d}")
			for n, hosts in sorted(per_node.items(), key=lambda kv: -kv[0]):
				label = f"  {n} GPU{'s' if n > 1 else ''}/node"
				lines.append(f"  {label:<22} {count(hosts)} {n * len(hosts):5d}  {compress_node_list(hosts)}")
		lines.append("")
	if topology["cpu_types"]:
		lines.append("Nodes without GPU by CPU type")
		lines.append(f"  {'CPU type':<26} {header}  Nodes")
		for cpu_type, hosts in sorted(topology["cpu_types"].items(), key=lambda kv: -len(kv[1])):
			lines.append(f"  {cpu_type:<26} {count(hosts)}  {compress_node_list(hosts)}")
		lines.append("")
	lines.append("Node classes (submitslurm -e excellent, -s strong, -x excludes weak)")
	lines.append(f"  {'Class':<12} {header}  Nodes")
	for name, hosts in topology["classes"].items():
		if hosts or name not in ("default", "unclassified"):
			line = f"  {name:<12} {count(hosts)}  {compress_node_list(hosts)}"
			if name == "unclassified":
				line += "  (" + "; ".join(sorted(topology["unknown_hardware"])) + ")"
			lines.append(line)
	unknown = topology["unknown_hardware"]
	if unknown:
		lines.append("")
		lines.append("Unclassified hardware (not in GPU_ALL/CPU_ALL of laupy/maxwell.py)")
		lines.append(f"  {'Hardware':<30} {header}  Nodes")
		for hardware, hosts in sorted(unknown.items(), key=lambda kv: (-len(kv[1]), kv[0])):
			lines.append(f"  {hardware:<30} {count(hosts)}  {compress_node_list(hosts)}")
	if from_sinfo and table:
		lines.extend(_format_table_comparison(topology))
	elif topology["not_described"]:
		lines.append("")
		lines.append(f"In sinfo but missing in {table_name} ({len(topology['not_described'])}): {compress_node_list(topology['not_described'])}")
	if workload:
		lines.append("")
		lines.extend(_format_workload(topology, verbose, user, color))
	if verbose:
		lines.append("")
		users_column = " Users (running jobs)" if workload else ""
		lines.append(f"  {'Host':<16} {'State':<8} {'Class':<12} {'Memory':<8} {'CPU':<26} {'GPU':<16} Partitions{users_column}")
		for node in topology["nodes"]:
			gpus = "+".join(f"{node['GPU'].count(t)}x{t}" for t in sorted(set(node["GPU"]))) if node["GPU"] else "-"
			state = node["state"] or "-"
			line = f"  {node['Host']:<16} {state:<8} {node['class']:<12} {node['Memory']:<8} {node['CPU']:<26} {gpus:<16} {' '.join(node['Partitions'])}"
			if workload:
				users = workload["node_users"].get(node["Host"], {})
				line += "  " + (", ".join(_mark(f"{u}({n})", u == user, color) for u, n in sorted(users.items())) if users else "-")
				if user in users:
					line = _mark(">", True, color) + line[1:]	# Own jobs on this node
			lines.append(line)
	return "\n".join(lines)


# Workload: topology plus idle nodes, the job queue and who runs where

QUERY_LOG = []	# (description, command, seconds) of every SLURM query of this process

def format_query_log():
	"""One line per SLURM query made so far: 'sinfo node features and states: 0.42 s'."""
	return [f"SLURM query {cmd.split()[0]} ({what}): {seconds:.2f} s" for what, cmd, seconds in QUERY_LOG]

def _run_slurm_query(cmd, description=None):
	"""stdout of a SLURM query command, timed in QUERY_LOG; RuntimeError with the last stderr line on failure."""
	import time
	start = time.monotonic()
	try:
		result = subprocess.run(cmd, capture_output=True, text=True)
	except OSError as e:
		raise RuntimeError(f"Cannot run {cmd[0]}: {e}")
	finally:
		QUERY_LOG.append((description or cmd[0], " ".join(cmd), time.monotonic() - start))
	if result.returncode != 0:
		messages = [line for line in result.stderr.splitlines() if line.strip()]
		raise RuntimeError(f"{cmd[0]} failed: {messages[-1] if messages else f'exit code {result.returncode}'}")
	return result.stdout

def get_node_cpu_load(partition=None):
	"""Per-node CPU allocation from sinfo: {"max-wn064": {"alloc": 32, "idle": 32, "other": 0, "total": 64}}."""
	cmd = ["sinfo", "-N", "-h", "-o", "%N %C"]
	partitions = partition_list(partition)
	if partitions:
		cmd += ["-p", ",".join(partitions)]
	load = {}
	for line in _run_slurm_query(cmd, "CPU load").splitlines():
		fields = line.split()
		if len(fields) >= 2 and fields[0] not in load:
			try:
				alloc, idle, other, total = (int(x) for x in fields[1].split("/"))
			except ValueError:
				continue
			load[fields[0]] = {"alloc": alloc, "idle": idle, "other": other, "total": total}
	return load

def array_task_count(job_id):
	"""Number of tasks a squeue job ID stands for: "123_[1-10,20%4]" -> 11, "123_4" or "123" -> 1."""
	match = re.search(r"_\[([^\]]+)\]", job_id)
	if not match:
		return 1
	count = 0
	for part in match.group(1).split("%")[0].split(","):
		span, _, step = part.partition(":")	# 1-99:2 is every second task
		bounds = span.split("-")
		try:
			count += (int(bounds[-1]) - int(bounds[0])) // (int(step) if step else 1) + 1
		except (ValueError, ZeroDivisionError):
			count += 1
	return max(count, 1)

def get_queue_jobs(partition=None):
	"""Running and pending jobs from squeue.

	Running jobs are taken from all partitions (other partitions share nodes with the asked ones),
	pending jobs only from the given partitions. Each job: {"job_id", "user", "state", "partition",
	"nodes" (list), "cpus", "reason", "tasks" (pending array tasks counted)}.
	"""
	fmt = "%i|%u|%T|%P|%N|%C|%r"
	running_cmd = ["squeue", "-h", "-t", "RUNNING,COMPLETING", "-o", fmt]
	pending_cmd = ["squeue", "-h", "-t", "PENDING", "-o", fmt]
	partitions = partition_list(partition)
	if partitions:
		pending_cmd += ["-p", ",".join(partitions)]
	jobs = []
	for cmd in (running_cmd, pending_cmd):
		for line in _run_slurm_query(cmd, "jobs").splitlines():
			fields = line.split("|")
			if len(fields) < 7:
				continue
			job_id, user, state, job_partition, nodelist, cpus, reason = (f.strip() for f in fields[:7])
			jobs.append({
				"job_id": job_id, "user": user, "state": state, "partition": job_partition,
				"nodes": nodes_from_string(nodelist) if nodelist and state != "PENDING" else [],
				"cpus": int(cpus) if cpus.isdigit() else 0,
				"reason": reason,
				"tasks": array_task_count(job_id) if state == "PENDING" else 1,
			})
	return jobs

def get_cluster_workload(partition=None, table_path=None):
	"""get_cluster_topology() plus the current load (listnodes --workload).

	Every node additionally gets "idle" (bool: up and no CPU allocated), "cpus_idle" and
	"cpus_total". The returned dict has an extra key "workload":
	  error          None, or why squeue/sinfo could not be queried
	  running_jobs   number of running jobs on the described nodes
	  running_users  {user: {"jobs": n, "nodes": [...], "pending": n}}, also for users only pending
	  pending_jobs   pending jobs in the partitions (array tasks counted)
	  pending_reasons, pending_partitions  {reason or partition: pending jobs}
	  node_users     {node: {user: running jobs}}
	"""
	topology = get_cluster_topology(partition=partition, live=True, table_path=table_path)
	workload = {"error": topology["live_error"], "running_jobs": 0, "running_users": {}, "pending_jobs": 0,
	            "pending_reasons": {}, "pending_partitions": {}, "node_users": {}}
	topology["workload"] = workload
	if workload["error"]:
		return topology
	try:
		if topology["source"] == "sinfo" and _live_rows is not None:
			load = {row["Host"]: row["cpu_load"] for row in _live_rows if row.get("cpu_load")}	# Same query
		else:
			load = get_node_cpu_load(topology["partitions"])
		jobs = get_queue_jobs(topology["partitions"])
	except RuntimeError as e:
		workload["error"] = str(e)
		return topology
	described = {node["Host"] for node in topology["nodes"]}
	for node in topology["nodes"]:
		cpus = load.get(node["Host"])
		node["cpus_idle"] = cpus["idle"] if cpus else None
		node["cpus_total"] = cpus["total"] if cpus else None
		node["idle"] = bool(node["up"] and cpus and cpus["alloc"] == 0 and node["state"].startswith("idle"))
	users = workload["running_users"]
	for job in jobs:
		info = users.setdefault(job["user"], {"jobs": 0, "nodes": [], "pending": 0})
		if job["state"] == "PENDING":
			info["pending"] += job["tasks"]
			workload["pending_jobs"] += job["tasks"]
			reason = job["reason"] or "None"
			workload["pending_reasons"][reason] = workload["pending_reasons"].get(reason, 0) + job["tasks"]
			workload["pending_partitions"][job["partition"]] = workload["pending_partitions"].get(job["partition"], 0) + job["tasks"]
			continue
		job_nodes = [n for n in job["nodes"] if n in described]
		if not job_nodes:
			continue # Running elsewhere in the cluster
		workload["running_jobs"] += 1
		info["jobs"] += 1
		for n in job_nodes:
			if n not in info["nodes"]:
				info["nodes"].append(n)
			per_node = workload["node_users"].setdefault(n, {})
			per_node[job["user"]] = per_node.get(job["user"], 0) + 1
	workload["running_users"] = {u: i for u, i in users.items() if i["jobs"] or i["pending"]}
	return topology

def _format_table_comparison(topology):
	"""Where maxwell.tsv differs from SLURM (sinfo source only)."""
	missing, stale = topology["not_described"], topology["not_in_sinfo"]
	differs, filled = topology["hardware_differs"], topology["filled_from_table"]
	if not (missing or stale or differs or filled):
		return ["", f"{os.path.basename(topology['table'])} agrees with SLURM for these nodes."]
	lines = ["", f"Compared with {topology['table']} (refresh it with listnodes --export-topology FILE)"]
	if missing:
		lines.append(f"  Missing in the table ({len(missing)}): {compress_node_list(missing)}")
	if stale:
		lines.append(f"  In the table but not in SLURM ({len(stale)}): {compress_node_list(stale)}")
	if differs:
		changes = {}
		for host, old, new in differs:
			changes.setdefault((old, new), []).append(host)
		lines.append(f"  Hardware differs ({len(differs)} nodes), table -> SLURM:")
		for (old, new), hosts in sorted(changes.items(), key=lambda kv: -len(kv[1])):
			lines.append(f"    {compress_node_list(hosts)}: {old} -> {new}")
	if filled:
		lines.append(f"  No hardware features in SLURM, taken from the table ({len(filled)}): {compress_node_list(filled)}")
	return lines

GREEN = "\033[32m"
RESET = "\033[0m"

def current_user():
	"""Login name of the current user (for marking own jobs)."""
	import getpass
	try:
		return getpass.getuser()
	except Exception:
		return os.environ.get("USER", "")

def _mark(text, mine, color):
	return f"{GREEN}{text}{RESET}" if mine and color else text

def _format_workload(topology, verbose, user=None, color=False):
	"""Workload section of format_cluster_topology()."""
	workload = topology["workload"]
	where = ",".join(topology["partitions"]) if topology["partitions"] else "all partitions"
	lines = [f"Queue ({where})"]
	if workload["error"]:
		lines.append(f"  Workload unavailable: {workload['error']}")
		return lines
	nodes = topology["nodes"]
	running_users = [u for u, i in workload["running_users"].items() if i["jobs"]]
	pending_users = [u for u, i in workload["running_users"].items() if i["pending"]]
	busy = [n["Host"] for n in nodes if n["Host"] in workload["node_users"]]
	cpus_total = sum(n["cpus_total"] or 0 for n in nodes if n["up"])
	cpus_idle = sum(n["cpus_idle"] or 0 for n in nodes if n["up"])
	lines.append(f"  Running jobs:  {workload['running_jobs']} on {len(busy)} nodes, {len(running_users)} users")
	lines.append(f"  Pending jobs:  {workload['pending_jobs']} (array tasks counted), {len(pending_users)} users")
	if cpus_total:
		lines.append(f"  Idle CPUs:     {cpus_idle} of {cpus_total} on nodes that are up ({100 * cpus_idle / cpus_total:.0f} %)")
	if workload["pending_reasons"]:
		reasons = sorted(workload["pending_reasons"].items(), key=lambda kv: -kv[1])
		lines.append("  Pending by reason:    " + ", ".join(f"{r} {n}" for r, n in reasons))
		partitions = sorted(workload["pending_partitions"].items(), key=lambda kv: -kv[1])
		lines.append("  Pending by partition: " + ", ".join(f"{p} {n}" for p, n in partitions))
	idle = [n for n in nodes if n.get("idle")]
	lines.append("")
	lines.append(f"Idle nodes ({len(idle)})")
	groups = {}
	for node in idle:
		key = "+".join(sorted(set(node["GPU"]))) if node["GPU"] else f"no GPU, {node['class']}"
		groups.setdefault(key, []).append(node["Host"])
	for key, hosts in sorted(groups.items(), key=lambda kv: -len(kv[1])):
		lines.append(f"  {key:<24} {len(hosts):5d}  {compress_node_list(hosts)}")
	partly = [n for n in nodes if n["up"] and not n.get("idle") and n.get("cpus_idle")]
	if partly:
		free = sum(n["cpus_idle"] for n in partly)
		lines.append(f"Partly used nodes with idle CPUs ({len(partly)}, {free} CPUs): {compress_node_list([n['Host'] for n in partly])}")
	mine = workload["running_users"].get(user) if user else None
	lines.append("")
	if mine:
		text = f"Your jobs ({user}): {mine['jobs']} running on {len(mine['nodes'])} nodes, {mine['pending']} pending"
		if mine["nodes"]:
			text += f": {compress_node_list(mine['nodes'])}"
		lines.append(_mark(text, True, color))
	else:
		lines.append(f"Your jobs ({user}): none in these partitions")
	if verbose and workload["running_users"]:
		lines.append("")
		lines.append(f"  {'User':<16} {'Running':>7} {'Nodes':>6} {'Pending':>8}")
		for name, info in sorted(workload["running_users"].items(), key=lambda kv: (-len(kv[1]["nodes"]), -kv[1]["pending"], kv[0])):
			lines.append(_mark(f"  {name:<16} {info['jobs']:7d} {len(info['nodes']):6d} {info['pending']:8d}", name == user, color))
	return lines
