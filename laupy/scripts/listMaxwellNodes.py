#!/usr/bin/env python3

import argparse
import os
import sys
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
    parser.add_argument('--list', metavar='FILE', dest='table',
                        help="Node table (maxwell.tsv format) to use with the live SLURM data: it fills what SLURM "
                             "does not say (FLOPS, warranty, hardware of nodes without features), resolves ambiguous "
                             "features and is compared with SLURM in --topology; where both have a value, SLURM wins. "
                             f"Default: ${maxwell.NODE_TABLE_ENV} or ~/maxwell.tsv if it exists")
    parser.add_argument('--live', action='store_true', help="Use live SLURM data only, ignore any node table")
    parser.add_argument('--offline', action='store_true',
                        help="With --topology, do not query SLURM: describe --list FILE or the node table found")
    parser.add_argument('--export-topology', metavar='FILE',
                        help="Write the node table (maxwell.tsv format) to FILE and exit, e.g. ~/maxwell.tsv: the nodes "
                             "SLURM reports, combined with --list FILE or the node table found (none with --live)")
    ARG = parser.parse_args()

    if ARG.live and (ARG.table or ARG.offline):
        parser.error("--live ignores node tables, it cannot be combined with --list or --offline")
    if ARG.table and not os.path.isfile(ARG.table):
        parser.error(f"--list: node table {ARG.table} does not exist")
    if ARG.offline and not (ARG.topology and not ARG.workload):
        parser.error("--offline only applies to --topology")
    if ARG.verbose and not (ARG.topology or ARG.workload):
        parser.error("--verbose only applies to --topology/--workload")
    # One source setup for all modes; the single sinfo query is shared by everything below
    maxwell.set_node_table(ARG.table, live=not ARG.offline, use_table=not ARG.live)

    if ARG.export_topology:
        try:
            count = maxwell.export_topology(os.path.expanduser(ARG.export_topology))
        except RuntimeError as e:
            print(f"Cannot export the topology: {e}", file=sys.stderr)
            sys.exit(1)
        print(f"Wrote {count} nodes to {os.path.expanduser(ARG.export_topology)}")
        return
    if ARG.workload:
        print(maxwell.format_cluster_topology(maxwell.get_cluster_workload(partition=ARG.partition), verbose=ARG.verbose))
        return
    if ARG.topology:
        topology = maxwell.get_cluster_topology(partition=ARG.partition, live=not ARG.offline)
        print(maxwell.format_cluster_topology(topology, verbose=ARG.verbose))
        return

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

