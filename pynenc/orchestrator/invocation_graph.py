from collections import defaultdict
from typing import Set


class InvocationGraph:
    """A directed acyclic graph representing the invocation dependencies"""

    def __init__(self) -> None:
        self.adj_list: dict = defaultdict(set)  # adjacency list representing the graph
        # mapping of invocations to the invocations they are waiting on
        self.waiting_for: dict = defaultdict(set)
        # mapping of invocations to the invocations waiting on them
        self.waited_by: dict = defaultdict(set)

    def add_edge(self, parent: str, child: str) -> bool:
        # Adds an edge to the graph if it does not create a cycle
        if self._causes_cycle(parent, child):
            return False
        self.adj_list[parent].add(child)
        return True

    def add_waiting_for(self, parent: str, child: str) -> None:
        # Registers that 'parent' is waiting for 'child'
        self.waiting_for[parent].add(child)
        self.waited_by[child].add(parent)

    def get_invocations_to_run(self, max_num_invocations: int) -> Set[str]:
        # Returns invocations that are not being waited on by any other invocation, up to 'max_num_invocations'
        invocations_to_run = set(
            invocation
            for invocation in self.waited_by
            if not self.waited_by[invocation]
        )
        return set(
            sorted(
                invocations_to_run, key=lambda i: len(self.waiting_for[i]), reverse=True
            )[:max_num_invocations]
        )

    def _causes_cycle(self, parent: str, child: str) -> bool:
        # Returns whether adding an edge from 'parent' to 'child' would cause a cycle
        visited: set = set()
        stack: set = set()
        return self._is_cyclic_util(parent, visited, stack)

    def _is_cyclic_util(self, node: str, visited: set, stack: set) -> bool:
        stack.add(node)
        visited.add(node)
        for neighbour in self.adj_list.get(node, []):
            if neighbour not in visited:
                if self._is_cyclic_util(neighbour, visited, stack):
                    return True
            elif neighbour in stack:
                return True
        stack.remove(node)
        return False
