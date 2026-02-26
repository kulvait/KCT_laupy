#!/usr/bin/env python

# Query Maxwell cluster nodes and categorize them based on hardware capabilities.

import re
import csv
import pprint
import os
import subprocess

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path to the Maxwell TSV file relative to the parent directory of the script
maxwell_tsv_path = os.path.join(script_dir, 'maxwell.tsv')

# Make the path absolute (this removes any '..' in the path)
maxwell_tsv_path = os.path.abspath(maxwell_tsv_path)

# Define hardware categories
GPU_AVAIL = ['GA100', 'RTX A6000', 'GH100', 'RTX 2080', 'H200', 'GV100GL', 'Quadro RTX', 'GP100GL']
GPU_EXCELLENT = ['GA100', 'GH100', 'H200']
GPU_STRONG = GPU_EXCELLENT + ['GV100GL']
GPU_WEAK = ['Quadro RTX', 'GP100GL']
CPU_EXCELLENT = ['AMD EPYC 7F52', 'AMD EPYC 7H12', 'AMD EPYC 9534', 'AMD EPYC 9374', 'Gold 6240 CPU @ 2.60GHz',
		'Gold 6234 CPU @ 3.30GHz', 'Gold 6226 CPU @ 2.70GHz', 'AMD EPYC 9654', 'AMD EPYC 7702',
		'AMD EPYC 7643', 'AMD EPYC 7543'
	]
CPU_STRONG = CPU_EXCELLENT + [
		'AMD EPYC 7513', 'AMD EPYC 7642', 'AMD EPYC 7542', 'AMD EPYC 7262', 'AMD EPYC 7402',
		'Gold 6148 CPU @ 2.40GHz', 'Gold 6126 CPU @ 2.60GHz', 'Gold 6140 CPU @ 2.30GHz', 'Gold 5218 CPU @ 2.30GHz',
		'Silver 4216 CPU @ 2.10GHz'
	]
CPU_WEAK = ['E5-2698 v4 @ 2.20GHz', 'E5-2640 v4 @ 2.40GHz', 'Silver 4210 CPU @ 2.20GHz', 'Silver 4214 CPU @ 2.20GHz',
		'Silver 4114 CPU @ 2.20GHz', 'Gold 5115 CPU @ 2.40GHz', 'Gold 6230 CPU @ 2.10GHz', 'Gold 5115 CPU @ 2.40GHz',
		'Silver 4214 CPU @ 2.20GHz'
	]

def parse_maxwell_tsv(file_path):
	with open(file_path, 'r') as f:
		reader = csv.DictReader(f, delimiter='\t')	# Use tab as delimiter
		rows = [row for row in reader]	# Convert all rows to a list of dicts
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
			partitions = row['Partitions'].strip()
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

def get_all_nodes(partition=None):
	init_node_cache()
	if partition is not None:
		# Filter ALL_NODES based on partition which can be string or list
		partitions = []
		if isinstance(partition, str):
			partitions = [p.strip() for p in partition.split(',')]
		elif not isinstance(partition, (list, tuple, set)):
			partitions = list(partition)
		else:
			raise ValueError(f"Invalid partition type: {type(partition)}")
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
	partitions = []
	if partition is not None:
		partitions = []
		if isinstance(partition, str):
			partitions = [p.strip() for p in partition.split(',')]
		elif not isinstance(partition, (list, tuple, set)):
			partitions = list(partition)
		else:
			raise ValueError(f"Invalid partition type: {type(partition)}")
		sinfo_cmd = ["sinfo", "-p", ",".join(partitions)]
	sinfo_output = run_command(sinfo_cmd)
	nodes = []
	for line in sinfo_output.splitlines()[1:]:	# Skip header
		if not idle_only or "idle" in line:
			nodes.append(line.split()[5])  # Assuming the node list is in the 6th column
	return nodes_from_string(",".join(nodes))

# Main logic of the script
def main():
	init_node_cache()
	maxwell_nodes = get_node_description()
	print("All Maxwell nodes:")
	pprint.pprint(maxwell_nodes)
	# List all GPU harware available on maxwell
	gpu_hardware = []
	for node in maxwell_nodes:
		for gpu in node['GPU']:
			gpu_hardware.append(gpu)
	gpu_hardware = list(set(gpu_hardware))# Unique GPU types	
			#Differentiate CPU only nodes later
# For nodes not in gpuall we list CPU architectures
	cpu_nodes = []
	for node in maxwell_nodes:
		node_name = node['Host']
		if 'allgpu' not in node['Partitions']:
			cpu_nodes.append(node)

	cpu_hardware = []
	for node in cpu_nodes:
		cpu_hardware.append(node['CPU'])
	cpu_hardware = list(set(cpu_hardware))# Unique CPU types
	print("Available CPU architectures on non-GPU nodes:")
	pprint.pprint(cpu_hardware)


	print("Available strong nodes:")
	pprint.pprint(",".join(STRONG_NODES))
	print("Weak nodes:")
	pprint.pprint(",".join(WEAK_NODES))
	print("Excellent GPU nodes:")
	pprint.pprint(",".join(get_excellent_gpu_nodes("allgpu")))

if __name__ == "__main__":
	main()









# Global variables for filtering
# Main logic of the script
def main():
	ALL = True
	STRONG = False
	WEAK = False
	EXCELENT = False
	
	# Argument parsing
	parser = argparse.ArgumentParser(description="Filter nodes based on categories")
	parser.add_argument('-i', '--idle', action='store_true', help="Select only idle nodes")
	parser.add_argument('-p', '--partition', type=str, default="allgpu", help="Partition to query (default: 'allgpu')")
	parser.add_argument('-a', '--all-nodes', action='store_true', help="Select all nodes (default)")
	parser.add_argument('-g', '--gpu-nodes', action='store_true', help="Select only nodes with GPUs")
	parser.add_argument('-s', '--strong-nodes', action='store_true', help="Select strong nodes")
	parser.add_argument('-w', '--weak-nodes', action='store_true', help="Select weak nodes")
	parser.add_argument('-e', '--excelent-nodes', action='store_true', help="Select A100 nodes")
	ARG = parser.parse_args()

	if ARG.strong_nodes:
		STRONG = True
		ALL = False
	if ARG.weak_nodes:
		WEAK = True
		ALL = False
	if ARG.excelent_nodes:
		EXCELENT = True
		ALL = False
	if ARG.all_nodes:
		ALL = True
		EXCELENT = STRONG = WEAK = False

	# Get list of all nodes from sinfo
	all_nodes = get_all_nodes(ARG.partition, ARG.idle)
	all_node_array = get_node_array(all_nodes)
	
	node_array = []

	# Determine which node groups to use for filtering
	if ALL:
		node_array = all_node_array
	else:
		if EXCELENT:
			node_array = maxwell.get_excellent_nodes()
		elif STRONG:
			node_array = maxwell.get_strong_nodes()
		elif WEAK:
			node_array = maxwell.get_weak_nodes()

	# Filter nodes based on selected groups
	filtered_nodes = [node for node in node_array if node in all_node_array]

	#Filter gpu nodes only if requested
	if ARG.gpu_nodes:
		gpu_nodes = maxwell.get_gpu_nodes()
		filtered_nodes = [node for node in filtered_nodes if node in gpu_nodes]
	#Make the list unique and sorted list
	filtered_nodes = sorted(set(filtered_nodes))

	# Output the resulting node list
	print(",".join(filtered_nodes))


if __name__ == "__main__":
	main()

