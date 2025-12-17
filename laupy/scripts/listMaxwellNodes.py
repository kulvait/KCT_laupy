#!/usr/bin/env python3

import subprocess
import argparse
import re
from laupy import maxwell

#Listing of active nodes as offered by sinfo

# Node definitions (change these lists as needed)
#WEAK_NODES = [
#    "max-p3ag[005-031]", "max-wng[004-007]", "max-exflg[007-024]"
#]
#
#STRONG_NODES = [
#    "max-hzgg002", "max-cdcsg001", "max-cssbg[011-023]", "max-mpag[001-003,008-013]",
#    "max-uhhg[001-013]", "max-wng024", "max-wng[037-042,052-053,060-062]",
#    "max-m001", "max-m002", "max-wn[130-137]", "max-wn[023-164]", "max-hzgg[005-006]"
#]
#
##A100 GPU or better nodes
#ANODES = [
#    "max-cdcsg001", "max-cfelg[008-011]", "max-cmsg011", "max-cssbg[011-023]",
#    "max-exflg[025-027]", "max-exflg[032-035]", "max-mpag[001-013]", "max-p3ag[032-035]", "max-wng[043-067]", "max-hpcgwg[001-008]"
#]
#
#STRONG_NODES.extend(ANODES)  # Include A100 nodes in strong nodes


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

# Get list of nodes in the partition (using sinfo)
def get_all_nodes(partition, idle_only = False):
    sinfo_cmd = ["sinfo", "-p", partition]
    sinfo_output = run_command(sinfo_cmd)
    nodes = []
    for line in sinfo_output.splitlines()[1:]:  # Skip header
        if not idle_only or "idle" in line:
            nodes.append(line.split()[5])  # Assuming the node list is in the 6th column
    return ",".join(nodes)

# Main function to process the nodes string
def get_node_array(nodesStr):
    # Regular expression to match the part inside square brackets and prevent premature splitting
    node_entries = re.findall(r'[^\[\],]+(?:\[[^\]]+\])?', nodesStr)
    node_array = []
    for entry in node_entries:
        expanded = expand_node_ranges(entry)
        node_array.extend(expanded)
    return node_array

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

