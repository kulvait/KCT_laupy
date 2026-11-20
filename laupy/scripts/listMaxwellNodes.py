#!/usr/bin/env python3

import argparse
from laupy import maxwell

# List Maxwell nodes by class, or describe the whole cluster with --topology

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
    parser.add_argument('-p', '--partition', type=str, default="allcpu,allgpu", help="Partition to query (default: 'allcpu,allgpu')")
    parser.add_argument('-a', '--all-nodes', action='store_true', help="Select all nodes (default)")
    parser.add_argument('-g', '--gpu-nodes', action='store_true', help="Select only nodes with GPUs")
    parser.add_argument('-s', '--strong-nodes', action='store_true', help="Select strong nodes")
    parser.add_argument('-w', '--weak-nodes', action='store_true', help="Select weak nodes")
    parser.add_argument('-e', '--excelent-nodes', action='store_true', help="Select A100 nodes")
    parser.add_argument('-t', '--topology', action='store_true',
                        help="Describe the cluster instead of listing nodes: GPU and CPU types, node classes and live "
                             "states from sinfo, limited to --partition (use -p '' for all partitions)")
    parser.add_argument('-W', '--workload', action='store_true',
                        help="Like --topology, plus idle nodes, the job queue (running/pending jobs, pending reasons) "
                             "and, with --verbose, the users running on each node")
    parser.add_argument('-v', '--verbose', action='store_true', help="With --topology/--workload, add one line per node (and per user)")
    parser.add_argument('--offline', action='store_true', help="With --topology, do not query sinfo (hardware from maxwell.tsv only)")
    ARG = parser.parse_args()

    if ARG.workload:
        if ARG.offline:
            parser.error("--workload needs live data, it cannot be combined with --offline")
        print(maxwell.format_cluster_topology(maxwell.get_cluster_workload(partition=ARG.partition), verbose=ARG.verbose))
        return
    if ARG.topology:
        topology = maxwell.get_cluster_topology(partition=ARG.partition, live=not ARG.offline)
        print(maxwell.format_cluster_topology(topology, verbose=ARG.verbose))
        return
    if ARG.verbose or ARG.offline:
        parser.error("--verbose and --offline only apply to --topology/--workload")

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
    all_node_array = maxwell.get_live_nodes(ARG.partition, ARG.idle)
    
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

