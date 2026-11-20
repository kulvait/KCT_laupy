#!/usr/bin/env python3

import argparse
import os
import sys
from laupy import maxwell
from laupy.cli import add_node_source_arguments, apply_node_source_arguments, print_node_notes

# List Maxwell nodes by class, or describe the whole cluster with --topology

# Main logic of the script
def main():
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
                        help="Like --topology, plus idle nodes, the job queue (running/pending jobs, pending reasons), "
                             "your own jobs (green in a terminal) and, with --verbose, the users running on each node")
    parser.add_argument('-v', '--verbose', action='store_true', help="With --topology/--workload, add one line per node (and per user)")
    add_node_source_arguments(parser, offline=True)
    parser.add_argument('--update-default-list', action='store_true',
                        help=f"Refresh the app-managed node table ({maxwell.DEFAULT_NODE_TABLE}) from SLURM now and exit")
    parser.add_argument('--update-list', metavar='FILE',
                        help="Refresh this node table from SLURM (created if missing) and exit: SLURM values win, the "
                             "table fills the rest, nodes SLURM does not report are kept")
    parser.add_argument('--export-topology', metavar='FILE',
                        help="Write the node table (maxwell.tsv format) to FILE and exit, e.g. ~/maxwell.tsv: the nodes "
                             "SLURM reports, combined with --list FILE or the node table found (none with --no-list)")
    ARG = parser.parse_args()

    if ARG.offline and not (ARG.topology and not ARG.workload):
        parser.error("--offline only applies to --topology")
    if ARG.verbose and not (ARG.topology or ARG.workload):
        parser.error("--verbose only applies to --topology/--workload")
    # One source setup for all modes; the single sinfo query is shared by everything below
    apply_node_source_arguments(parser, ARG)

    if ARG.update_default_list or ARG.update_list:
        try:
            path, count, _ = maxwell.update_node_table(ARG.update_list)	# None: the app-managed table
        except (RuntimeError, OSError) as e:
            print(f"Cannot update the node table: {e}", file=sys.stderr)
            sys.exit(1)
        print(f"Updated {path} ({count} nodes)")
        return
    if ARG.export_topology:
        try:
            count = maxwell.export_topology(os.path.expanduser(ARG.export_topology))
        except RuntimeError as e:
            print(f"Cannot export the topology: {e}", file=sys.stderr)
            sys.exit(1)
        print(f"Wrote {count} nodes to {os.path.expanduser(ARG.export_topology)}")
        return
    if ARG.workload:
        color = sys.stdout.isatty() and not os.environ.get("NO_COLOR")	# Own jobs in green
        print(maxwell.format_cluster_topology(maxwell.get_cluster_workload(partition=ARG.partition),
                                              verbose=ARG.verbose, color=color))
        if ARG.verbose:
            print("\n".join([""] + maxwell.format_query_log()))
        return
    if ARG.topology:
        topology = maxwell.get_cluster_topology(partition=ARG.partition, live=not ARG.offline)
        print(maxwell.format_cluster_topology(topology, verbose=ARG.verbose))
        if ARG.verbose:
            print("\n".join([""] + maxwell.format_query_log()))
        return

    node_class = None
    if not ARG.all_nodes:
        # Same precedence as before: -e over -s over -w; -a lists all nodes
        node_class = "excellent" if ARG.excelent_nodes else ("strong" if ARG.strong_nodes else ("weak" if ARG.weak_nodes else None))
    filtered_nodes = maxwell.select_nodes(ARG.partition, node_class=node_class, gpu_only=ARG.gpu_nodes,
                                          state="idle" if ARG.idle else "any")

    # Output the resulting node list
    print(",".join(filtered_nodes))


def run():
    """Entry point: main() and then the notes about the node table (on stderr, output stays clean)."""
    try:
        main()
    finally:
        print_node_notes(file=sys.stderr)


if __name__ == "__main__":
    run()

