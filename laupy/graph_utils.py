#!/usr/bin/env python

"""
Small directed graph implementation and utilities.

Year: 2026
License: GNU GPL3
"""

from collections import deque
from typing import Dict, Iterable, Iterator, List, Set, Tuple, Hashable, Optional
from heapq import heappop, heappush

Node = Hashable
Edge = Tuple[Node, Node]


class CycleError(ValueError):
    """Raised when a cycle is detected in a directed graph."""


class DirectedGraph:
    """Minimal directed graph (no external deps)."""

    def __init__(self, edges: Optional[Iterable[Edge]] = None):
        self._succ: Dict[Node, Dict[Node, None]] = {}
        self._pred: Dict[Node, Dict[Node, None]] = {}
        if edges:
            self.add_edges_from(edges)

    def __contains__(self, node: Node) -> bool:
        return node in self._succ

    def __iter__(self) -> Iterator[Node]:
        return iter(self._succ)

    def __len__(self) -> int:
        return len(self._succ)

    def add_node(self, node: Node) -> None:
        if node not in self._succ:
            self._succ[node] = {}
            self._pred[node] = {}

    def add_edge(self, src: Node, dst: Node) -> None:
        self.add_node(src)
        self.add_node(dst)
        self._succ[src][dst] = None
        self._pred[dst][src] = None

    def add_edges_from(self, edges: Iterable[Edge]) -> None:
        for src, dst in edges:
            self.add_edge(src, dst)

    def successors(self, node: Node) -> Iterator[Node]:
        return iter(self._succ[node])

    def predecessors(self, node: Node) -> Iterator[Node]:
        return iter(self._pred[node])


def ancestors(graph: DirectedGraph, source: Node) -> Set[Node]:
    """Return all ancestors of source (excluding source)."""
    if source not in graph:
        raise KeyError(f"Node {source!r} not in graph")

    seen = {source}
    stack = [source]
    while stack:
        cur = stack.pop()
        for parent in graph.predecessors(cur):
            if parent not in seen:
                seen.add(parent)
                stack.append(parent)
    seen.remove(source)
    return seen




def is_directed_acyclic_graph(graph: DirectedGraph) -> bool:
    try:
        topological_sort(graph)
        return True
    except CycleError:
        return False


def find_cycle(graph: DirectedGraph) -> List[Edge]:
    """Return one cycle as edge list [(u,v), ...], raise CycleError if none."""
    done = set()

    for start in graph:
        if start in done:
            continue

        path = [start]
        active = {start: 0}
        stack = [iter(graph.successors(start))]

        while stack:
            it = stack[-1]
            try:
                nxt = next(it)
            except StopIteration:
                stack.pop()
                node = path.pop()
                active.pop(node, None)
                done.add(node)
                continue

            if nxt in active:
                cyc_nodes = path[active[nxt]:] + [nxt]
                return list(zip(cyc_nodes, cyc_nodes[1:]))

            if nxt not in done:
                active[nxt] = len(path)
                path.append(nxt)
                stack.append(iter(graph.successors(nxt)))

    raise CycleError("No cycle found")

def topological_sort_fifo(graph: DirectedGraph) -> List[Node]:
    """Return nodes in topological order. Raise CycleError on cycle."""
    indegree = {n: len(graph._pred[n]) for n in graph}
    q = deque([n for n in graph if indegree[n] == 0])
    out: List[Node] = []

    while q:
        n = q.popleft()
        out.append(n)
        for child in graph.successors(n):
            indegree[child] -= 1
            if indegree[child] == 0:
                q.append(child)

    if len(out) != len(graph):
        raise CycleError("Graph contains a cycle")
    return out

def topological_sort(graph: DirectedGraph, tie_breaking_priority=None):
    """
    Return nodes in topological order.

    Purpose of tie_breaking_priority:
    ---------------------------------
    Dependencies define only a partial order. At runtime, multiple nodes can be
    "ready" (in-degree 0) simultaneously and are not related by dependencies.
    In that case, this mapping lets callers pick a stable preferred order.

    tie_breaking_priority:
      dict[node] -> comparable value (commonly a tuple)

    Lower value wins. Nodes not present in the mapping use default ().
    """
    if tie_breaking_priority is None:
        return topological_sort_fifo(graph)

    indegree = {n: sum(1 for _ in graph.predecessors(n)) for n in graph}
    insertion_index = {n: i for i, n in enumerate(graph)}
    heap = []

    def enqueue(node):
        # We compute priority once when node becomes ready.
        # Heap items:
        #   1) caller-provided priority among independent ready nodes
        #   2) insertion_index for deterministic final tie-break
        #   3) node identity
        priority = tie_breaking_priority.get(node, ())
        heappush(heap, (priority, insertion_index[node], node))

    for node in graph:
        if indegree[node] == 0:
            enqueue(node)

    result = []
    while heap:
        _, _, node = heappop(heap)
        result.append(node)
        for child in graph.successors(node):
            indegree[child] -= 1
            if indegree[child] == 0:
                enqueue(child)

    if len(result) != len(indegree):
        raise CycleError("Graph contains a directed cycle")

    return result
