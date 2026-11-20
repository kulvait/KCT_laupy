#!/usr/bin/env python

# Query Maxwell cluster nodes and categorize them based on hardware capabilities.

import re
import csv
import os
import subprocess

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path to the Maxwell TSV file relative to the parent directory of the script
maxwell_tsv_path = os.path.join(script_dir, 'maxwell.tsv')

# Make the path absolute (this removes any '..' in the path)
maxwell_tsv_path = os.path.abspath(maxwell_tsv_path)

# Define hardware categories
GPU_AVAIL = ['GA100', 'RTX A6000', 'GH100', 'RTX 2080', 'H200', 'GV100GL', 'Quadro RTX', 'GP100GL', 'GB202GL RTX PRO']
GPU_EXCELLENT = ['GA100', 'GH100', 'H200']
GPU_STRONG = GPU_EXCELLENT + ['GV100GL', 'GB202GL RTX PRO']
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

_node_cache = None
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

def init_node_cache():
	global _node_cache
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
		_node_cache = parse_maxwell_tsv(maxwell_tsv_path)
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
	try:
		result = subprocess.run(sinfo_cmd, capture_output=True, text=True)
	except OSError as e:
		raise RuntimeError(f"Cannot run sinfo: {e}")
	if result.returncode != 0:
		messages = [line for line in result.stderr.splitlines() if line.strip()]
		raise RuntimeError(f"sinfo failed: {messages[-1] if messages else f'exit code {result.returncode}'}")
	states = {}
	for line in result.stdout.splitlines():
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
	"""Class of a maxwell.tsv row, as used by submitslurm -e/-s/-x.

	excellent, strong or weak from the hardware lists; default when the hardware is known
	(in GPU_ALL/CPU_ALL) but in none of these classes; unclassified when it is not in GPU_ALL/CPU_ALL.
	"""
	init_node_cache()
	host = node["Host"]
	for name, members in (("excellent", EXCELLENT_NODES), ("strong", STRONG_NODES), ("weak", WEAK_NODES)):
		if host in members:
			return name
	return "unclassified" if unknown_hardware(node) else "default"

def get_cluster_topology(partition=None, live=True):
	"""Describe the cluster: hardware from maxwell.tsv, optionally with live node states from sinfo.

	partition: limit to these partitions (str "allcpu,allgpu", list, or None for all).
	live: query sinfo; if that fails, the topology has live_error set and no states.
	Returns a dict:
	  partitions       the partitions asked for ([] = all)
	  live_error       None, or why sinfo could not be queried (live states missing)
	  nodes            [{"Host", "CPU", "GPU" (list), "Memory", "CPUCount", "Partitions", "Warranty",
	                     "class", "state" (None without live data), "up" (bool or None)}]
	  gpu_types        {gpu_type: {"nodes": [...], "gpus": count}} for nodes with GPUs
	  cpu_types        {cpu_type: [nodes]} for nodes without GPUs
	  classes          {"excellent"|"strong"|"weak"|"default"|"unclassified": [nodes]}, see node_class()
	  not_described    nodes reported by sinfo but missing in maxwell.tsv
	  unknown_hardware {"GPU X"|"CPU Y": [nodes]} hardware in no list of this module (the unclassified nodes)
	"""
	init_node_cache()
	partitions = partition_list(partition)
	states = {}
	live_error = None
	if live:
		try:
			states = get_live_node_states(partitions)
		except RuntimeError as e:
			live_error = str(e)
	nodes = []
	for row in _node_cache:
		if partitions and not any(p in row["Partitions"] for p in partitions):
			continue
		state = states.get(row["Host"]) if live and live_error is None else None
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
			"up": node_is_up(state) if live and live_error is None else None,
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
	described = {row["Host"] for row in _node_cache}
	return {
		"partitions": partitions,
		"live_error": live_error,
		"nodes": nodes,
		"gpu_types": gpu_types,
		"cpu_types": cpu_types,
		"classes": classes,
		"not_described": sorted(n for n in states if n not in described),
		"unknown_hardware": unknown,
	}

def format_cluster_topology(topology, verbose=False):
	"""Human readable report of get_cluster_topology(); verbose adds one line per node."""
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
	if topology["live_error"]:
		lines.append(f"Live node states unavailable: {topology['live_error']}")
	elif not live:
		lines.append("Live node states not queried.")
	lines.append(f"Described in maxwell.tsv: {len(topology['nodes'])} nodes")
	if live:
		states = {}
		for node in topology["nodes"]:
			key = node["state"] if node["state"] is not None else "not in sinfo"
			states[key] = states.get(key, 0) + 1
		up = sum(1 for node in topology["nodes"] if node["up"])
		lines.append(f"Up: {up}; states: " + ", ".join(f"{k} {v}" for k, v in sorted(states.items(), key=lambda kv: -kv[1])))
	lines.append("")
	lines.append("GPU nodes by GPU type")
	lines.append(f"  {'GPU type':<16} {header}  GPUs  Nodes")
	for gpu_type, info in sorted(topology["gpu_types"].items(), key=lambda kv: -len(kv[1]["nodes"])):
		lines.append(f"  {gpu_type:<16} {count(info['nodes'])} {info['gpus']:5d}  {compress_node_list(info['nodes'])}")
	lines.append("")
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
	if topology["not_described"]:
		lines.append("")
		lines.append(f"In sinfo but missing in maxwell.tsv ({len(topology['not_described'])}): {compress_node_list(topology['not_described'])}")
	if workload:
		lines.append("")
		lines.extend(_format_workload(topology, verbose))
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
				line += "  " + (", ".join(f"{u}({n})" for u, n in sorted(users.items())) if users else "-")
			lines.append(line)
	return "\n".join(lines)


# Workload: topology plus idle nodes, the job queue and who runs where

def _run_slurm_query(cmd):
	"""stdout of a SLURM query command; RuntimeError with the last stderr line on failure."""
	try:
		result = subprocess.run(cmd, capture_output=True, text=True)
	except OSError as e:
		raise RuntimeError(f"Cannot run {cmd[0]}: {e}")
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
	for line in _run_slurm_query(cmd).splitlines():
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
		for line in _run_slurm_query(cmd).splitlines():
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

def get_cluster_workload(partition=None):
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
	topology = get_cluster_topology(partition=partition, live=True)
	workload = {"error": topology["live_error"], "running_jobs": 0, "running_users": {}, "pending_jobs": 0,
	            "pending_reasons": {}, "pending_partitions": {}, "node_users": {}}
	topology["workload"] = workload
	if workload["error"]:
		return topology
	try:
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
			continue	# Running elsewhere in the cluster
		workload["running_jobs"] += 1
		info["jobs"] += 1
		for n in job_nodes:
			if n not in info["nodes"]:
				info["nodes"].append(n)
			per_node = workload["node_users"].setdefault(n, {})
			per_node[job["user"]] = per_node.get(job["user"], 0) + 1
	workload["running_users"] = {u: i for u, i in users.items() if i["jobs"] or i["pending"]}
	return topology

def _format_workload(topology, verbose):
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
	if verbose and workload["running_users"]:
		lines.append("")
		lines.append(f"  {'User':<16} {'Running':>7} {'Nodes':>6} {'Pending':>8}")
		for user, info in sorted(workload["running_users"].items(), key=lambda kv: (-len(kv[1]["nodes"]), -kv[1]["pending"], kv[0])):
			lines.append(f"  {user:<16} {info['jobs']:7d} {len(info['nodes']):6d} {info['pending']:8d}")
	return lines
