import uuid

import dask
import networkx as nx
import pandas as pd
from dask.dot import (box_label, get_dependencies, ishashable, istask,
                      key_split, name)

from .helper import _to_tuple


def to_nx(dsk):
    """
    Code mainly identical to dask.dot.to_graphviz and kept compatible.

    """

    collapse_outputs = False
    verbose = False
    data_attributes = {}
    function_attributes = {}

    g = nx.DiGraph()

    seen = set()
    connected = set()

    for k, v in dsk.items():
        k_name = name(k)
        if istask(v):
            func_name = name((k, "function")) if not collapse_outputs else k_name
            if collapse_outputs or func_name not in seen:
                seen.add(func_name)
                attrs = function_attributes.get(k, {}).copy()
                attrs.setdefault("label", key_split(k))
                attrs.setdefault("shape", "circle")
                g.add_node(func_name, **attrs)
            if not collapse_outputs:
                g.add_edge(func_name, k_name)
                connected.add(func_name)
                connected.add(k_name)

            for dep in get_dependencies(dsk, k):
                dep_name = name(dep)
                if dep_name not in seen:
                    seen.add(dep_name)
                    attrs = data_attributes.get(dep, {}).copy()
                    attrs.setdefault("label", box_label(dep, verbose))
                    attrs.setdefault("shape", "box")
                    g.add_node(dep_name, **attrs)
                g.add_edge(dep_name, func_name)
                connected.add(dep_name)
                connected.add(func_name)

        elif ishashable(v) and v in dsk:
            v_name = name(v)
            g.add_edge(v_name, k_name)
            connected.add(v_name)
            connected.add(k_name)

        if (not collapse_outputs or k_name in connected) and k_name not in seen:
            seen.add(k_name)
            attrs = data_attributes.get(k, {}).copy()
            attrs.setdefault("label", box_label(k, verbose))
            attrs.setdefault("shape", "box")
            g.add_node(k_name, **attrs)
    assert nx.dag.is_directed_acyclic_graph(g)
    return g


def get_time_deltas(dsk, df_tasks):
    """
    Calculate the maximum time between a node starting and all predecessor
    nodes finishing computation.

    With this function the critical paths of a dask task graph can be
    visualized. Ideally all predecessor node finish their computation
    at the same time. The critical path of the graph is responsible
    for the computation time.

    Parameters
    ----------
    dsk: dict
        Dask task graph.
    df_tasks: pd.DataFrame
        DataFrame of the dask task stream data. "key" column is mandatory to
        assign a row of the DataFrame to a node in the graph. "key" column
        must be of type string even when key is a tuple, because otherwise
        the type is not compatible with formats like parquet.

    Returns
    -------

    """
    # Convert dask task graph to nx
    g = to_nx(dsk)
    # Convert key to networkx node name and index df_tasks by that name
    df_tasks = df_tasks[df_tasks["key"].isin([str(key) for key in dsk.keys()])].copy()
    df_tasks.loc[df_tasks["action"] == "compute", "nx_name"] = df_tasks["key"].map(
        lambda item: dask.dot.name((_to_tuple(item), "function"))
    )
    df_tasks_nx = df_tasks.loc[df_tasks["action"] == "compute"].set_index("nx_name")

    max_time_deltas = dict()
    # Iterate over all task nodes
    for node in df_tasks_nx.index:
        successor_start = df_tasks_nx.loc[node, "start"]
        time_deltas = [pd.Timedelta(0)]
        # Iterate over all data predecessors of task node
        for data_predecessor in g.predecessors(node):
            try:
                # Get the task node predecessor of the data node
                task_predecessor = next(g.predecessors(data_predecessor))
                predecessor_stop = df_tasks_nx.loc[task_predecessor, "stop"]
                # Calculate the time delta
                time_delta = successor_start - predecessor_stop
                time_deltas.append(time_delta)
            except StopIteration:
                pass
        # Get the max time delta
        max_time_deltas[node] = max(time_deltas).total_seconds()
    return max_time_deltas


def max_dag_concurrency(graph):
    """
    Get the maximum number of concurrent tasks.
    """
    return max(_dag_concurrency(graph)[1])


def _dag_concurrency(graph):
    """
    Get number of concurrent tasks and corresponding node.

    """

    graph = graph.copy()
    # get start and end node
    n_predecessors_dict = dict()
    n_successors_dict = dict()
    for node in graph.nodes:
        n_predecessors_dict[node] = len(list(graph.predecessors(node)))
        n_successors_dict[node] = len(list(graph.successors(node)))
    # Connect nodes with no predecessors to origin node and with successors to end node
    # Give node of origin and end need an unique name
    origins = [key for key, value in n_predecessors_dict.items() if value == 0]
    origin = "origin_" + uuid.uuid4().hex[:8]
    for node in origins:
        graph.add_edge(origin, node)
    ends = [key for key, value in n_successors_dict.items() if value == 0]
    end = "end_" + uuid.uuid4().hex[:8]
    for node in ends:
        graph.add_edge(node, end)
    # get blocking nodes and difference of in- and output edges
    edge_sum = dict()
    blocking = dict()
    for node in graph.nodes:
        edge_sum[node] = len(list(graph.successors(node))) - len(
            list(graph.predecessors(node))
        )
        blocking[node] = list(graph.predecessors(node))
    visited = [origin]
    next_node = origin
    concurrency = edge_sum[next_node]
    concurrency_list = [concurrency]
    # Calculate sorted list of nodes.
    sorted_nodes = sorted(edge_sum, key=edge_sum.get, reverse=True)
    # Iterate over nodes until reaching the end node
    while next_node != end:
        # Remove visited nodes from blocking nodes
        for node, blocking_nodes in blocking.items():
            blocking[node] = [
                blocking_node
                for blocking_node in blocking_nodes
                if blocking_node not in visited
            ]
        # Remove nodes which don't have any blocking nodes
        blocking = {
            node: blocked_nodes
            for node, blocked_nodes in blocking.items()
            if len(blocked_nodes) != 0
        }
        # Select the first item of sorted_nodes
        # (highest number of successors - predecessors)
        # which was not yet visited and is not blocked
        next_node = [
            node
            for node in sorted_nodes
            if (node not in blocking.keys()) and (node not in visited)
        ][0]
        # Update concurrency and add next_node to visited nodes
        concurrency += edge_sum[next_node]
        concurrency_list.append(concurrency)
        visited.append(next_node)
    return visited, concurrency_list
