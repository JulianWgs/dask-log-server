import threading
import time
import datetime
import json
import yaml
import pytz
import pathlib
import uuid
import pickle
import base64
import random
import ast
import glob
import filecmp
import functools
import logging

import numpy as np
import pandas as pd
import distributed
import dask
from dask.dot import name, istask, key_split, get_dependencies, box_label, ishashable
import networkx as nx

# import dask.dot
import dask.dataframe as dd
import dask.bag as db

LOG_SCHMEMA_JSONL_SINGLE = (
    '{"datetime": "%(asctime)s","user_name": "%(name)s", '
    + '"type": "%(levelname)s","message": "%(message)s", "client_id": %client_id}'
)


def graph_logger_config(get, log_path):
    def graph_logger(*args, **kwargs):
        with open(f"{log_path}graph_{uuid.uuid4().hex[:16]}.dsk", "wb") as file:
            # TODO: Add case when only key word arguments are given
            dsk = _strip_instances(args[0])
            file.write(pickle.dumps(distributed.protocol.serialize(dsk)))
        return get(*args, **kwargs)

    return graph_logger


def dask_logger_config(
    time_interval=60.0,
    info_interval=1.0,
    log_path="logs/",
    n_tasks_min=1,
    filemode="a",
    additional_info=None,
    config_path=None,
    additional_logger_names=None,
):
    """
    Configure the dask logger to your liking.

    Parameters
    ----------
    info_interval: float
        Time in seconds between writing info log.
    time_interval: float
        Time in seconds between writing tasks log. Note that tasks will only
        be written if the number of tasks is above n_tasks_min. The default are 60 seconds.
    log_path: str
        Path to write the logging files. Both tasks and graphs are saved to the same folder. The default is "logs/".
    n_tasks_min: int
        Minimum number of tasks to write to the logging folder. The default is 1.
    filemode: str
        "a" (append): Append task logs to the same file in one session.
        "w" (write): Write a new file for every log.
        The default is "a".
    additional_info: json serializable object
        Additional information which is written into the version log with the key "additional_info".
    config_path: json serializable object
        Dask config path (https://docs.dask.org/en/latest/configuration.html).
    additional_logger_names: list of str
        List of logger names which will also be logged into the logging directory. The file format is jsonl. To get a
        list of all available logger use `[logging.getLogger(name).name for name in logging.root.manager.loggerDict]`.
        This is useful, when for example libraries like dask_ml log important information about model training.

    Examples
    --------
    >>> import dask
    >>> from dask import delayed
    >>> from distributed import Client
    >>> from dask_log_server import dask_logger_config
    >>> logger = dask_logger_config(
    ...     time_interval=1,
    ...     log_path="logs/",
    ...     n_tasks_min=1,
    ...     filemode="w",
    ...     additional_info={"instance_type": "c5.large"},
    ...     additional_logger_names=["dask_ml"],
    ... )
    >>> client = Client(
    ...     extensions=[logger]
    ... )

    >>> @delayed
    ... def add(a, b):
    ...     return a + b
    ...
    >>> @delayed
    ... def inc(a):
    ...     return a + 1
    ...
    >>> dask.compute(add(inc(2), 3))


    Returns
    -------
    dask_logger

    """

    def dask_logger(dask_client):
        pathlib.Path(log_path).mkdir(parents=True, exist_ok=True)

        config_logger(dask_client, log_path, config_path)

        dask_client.versions_logger = threading.Thread(
            target=versions_logger, args=(dask_client, log_path, additional_info)
        )
        dask_client.versions_logger.start()

        dask_client.task_logger = threading.Thread(
            target=task_logger,
            args=(dask_client, log_path, time_interval, n_tasks_min, filemode),
        )
        dask_client.task_logger.do_run = True
        dask_client.task_logger.force_log = False
        dask_client.task_logger.start()

        dask_client.info_logger = threading.Thread(
            target=info_logger, args=(dask_client, log_path, info_interval, filemode)
        )
        dask_client.info_logger.do_run = True
        dask_client.info_logger.start()

        if additional_logger_names:
            logger_logger(id(dask_client), log_path, additional_logger_names)

        dask_client.get = graph_logger_config(dask_client.get, log_path=log_path)

        return dask_client

    return dask_logger


def config_logger(dask_client, log_path, config_path=None):
    if config_path is None:
        config_path = f"{pathlib.Path.home()}/.config/dask/"
    configs = dict()
    for filename in glob.glob(config_path + "*.yaml"):
        with open(filename) as file:
            configs[filename.split("/")[-1]] = yaml.load(file, yaml.SafeLoader)
    log_message = {
        "datetime": str(datetime.datetime.now(pytz.utc)),
        "status": dask_client.status,
        "client_id": str(id(dask_client)),
        "configs": configs,
    }
    unique_id = uuid.uuid4().hex[:16]
    with open(f"{log_path}configs_{unique_id}.json", "w") as file:
        json.dump(log_message, file)


def versions_logger(dask_client, log_path, additional_info=None):
    while dask_client.status != "running":
        time.sleep(1)
    log_message = {
        "datetime": str(datetime.datetime.now(pytz.utc)),
        "status": dask_client.status,
        "client_id": str(id(dask_client)),
        "versions": dask_client.get_versions(),
    }
    unique_id = uuid.uuid4().hex[:16]
    if additional_info is not None:
        log_message["versions"]["additional_info"] = additional_info
    with open(f"{log_path}versions_{unique_id}.json", "w") as file:
        json.dump(log_message, file)


def task_logger(dask_client, log_path, time_interval, n_tasks_min, filemode):
    thread = threading.currentThread()
    last_time = time.time()
    unique_id = uuid.uuid4().hex[:16]
    while getattr(thread, "do_run", True):
        if dask_client.status == "running":
            now_time = time.time()
            tasks = dask_client.get_task_stream(last_time, now_time)
            if (
                (len(tasks) >= n_tasks_min)
                or getattr(thread, "force_log", False)
                and (len(tasks) >= 1)
            ):
                thread.force_log = False
                last_time = now_time
                # Make type json serializable
                [
                    task.update({"type": base64.b64encode(task["type"]).decode()})
                    for task in tasks
                ]
                log_message = {
                    "datetime": str(datetime.datetime.now(pytz.utc)),
                    "status": dask_client.status,
                    "client_id": str(id(dask_client)),
                    "tasks": tasks,
                }
                if filemode == "w":
                    unique_id = uuid.uuid4().hex[:16]
                with open(f"{log_path}tasks_{unique_id}.jsonl", filemode) as file:
                    file.write(json.dumps(log_message))
                    file.write("\n")
        time.sleep(time_interval)


def info_logger(dask_client, log_path, info_interval, filemode):
    thread = threading.currentThread()
    unique_id = uuid.uuid4().hex[:16]
    while getattr(thread, "do_run", True):
        if dask_client.status == "running":
            log_message = {
                "datetime": str(datetime.datetime.now(pytz.utc)),
                "status": dask_client.status,
                "client_id": str(id(dask_client)),
                "info": dask_client.scheduler_info(),
            }
            if filemode == "w":
                unique_id = uuid.uuid4().hex[:16]
            with open(f"{log_path}info_{unique_id}.jsonl", filemode) as file:
                file.write(json.dumps(log_message))
                file.write("\n")
        time.sleep(info_interval)


def logger_logger(client_id, log_path, logger_names):
    assert isinstance(client_id, int)
    assert isinstance(logger_names, (list, tuple))
    unique_id = uuid.uuid4().hex[:16]
    log_formatter = logging.Formatter(
        LOG_SCHMEMA_JSONL_SINGLE.replace("%client_id", str(client_id))
    )
    for logger_name in logger_names:
        logger = logging.getLogger(logger_name)
        handler = logging.FileHandler(
            f"{log_path}logger_{logger_name}_{unique_id}.jsonl"
        )
        handler.setFormatter(log_formatter)
        logger.addHandler(handler)


def read_tasks_raw(log_path):
    """
    Read tasks from one or multiple task log files.

    """
    df_tasks = (
        db.read_text(log_path + "/task*.jsonl")
        .map(str.split, "\n")
        .flatten()
        .filter(lambda item: item != "")
        .map(json.loads)
        .map(_flatten_dict, "tasks", ["datetime", "client_id"])
        .flatten()
        .map(
            _flatten_dict,
            "startstops",
            [
                "worker",
                "status",
                "nbytes",
                "thread",
                "type",
                "typename",
                "key",
                "datetime",
                "client_id",
            ],
        )
        .flatten()
        .to_dataframe(
            {
                "action": str,
                "start": "float64",
                "stop": "float64",
                "worker": str,
                "status": str,
                "nbytes": "int64",
                "thread": "int64",
                "type": str,
                "typename": str,
                "key": object,
                "datetime": str,
                "client_id": "int64",
            }
        )
    )

    for column_name in ["start", "stop"]:
        df_tasks[column_name] = dd.to_datetime(df_tasks[column_name], unit="s")
    df_tasks["datetime"] = dd.to_datetime(df_tasks["datetime"], utc=True)
    for column_name in ["action", "status"]:
        df_tasks[column_name] = df_tasks[column_name].astype("category")

    return df_tasks


def write_everything(log_path):
    """Read all logging data and write it as parquet."""
    df_tasks, df_graphs, df_clients = read_everything(log_path)

    dask.compute(
        [
            df_tasks.to_parquet(log_path + "/tasks.parquet", compute=False),
            df_graphs.to_parquet(log_path + "/graphs.parquet", compute=False),
            df_clients.to_parquet(log_path + "/clients.parquet", compute=False),
        ]
    )


def read_everything(log_path):
    """Read all logging data from logging directory."""

    df_tasks = read_tasks(log_path)
    df_graphs = read_graphs(df_tasks)
    df_versions = read_versions(log_path)
    df_client = read_client(df_tasks, df_versions)

    return df_tasks, df_graphs, df_client


def read_client(df_tasks, df_versions):
    """Read per client information."""
    df_tasks_group = df_tasks.groupby("client_id")
    df_starts = df_tasks_group["start"].min()
    df_stops = df_tasks_group["stop"].max()
    df_clients = dd.concat(
        [
            df_starts,
            df_stops,
            (df_stops - df_starts).rename("duration"),
            df_tasks_group["worker"].nunique().rename("n_used_workers"),
            df_tasks_group["id"].nunique().rename("n_graphs"),
            df_tasks_group["status"].count().rename("n_tasks"),
        ],
        axis=1,
        ignore_unknown_divisions=True,
    )
    df_clients = df_clients.merge(
        df_versions.drop(columns=["datetime", "status", "versions"]).rename(
            columns={"n_workers": "n_available_worker"}
        ),
        left_index=True,
        right_on="client_id",
    )
    return df_clients


def read_graphs(df_tasks):
    df_tasks["func_name"] = df_tasks["key"].map(dask.dot.label).map(_func_name)
    df_tasks.groupby("id")
    df_tasks_group = df_tasks.groupby("id")
    df_starts = df_tasks_group["start"].min()
    df_stops = df_tasks_group["stop"].max()
    df_graphs = dd.concat(
        [
            df_starts,
            df_stops,
            (df_stops - df_starts).rename("duration"),
            df_tasks_group["worker"].nunique().rename("n_workers"),
            df_tasks_group["status"].count().rename("n_tasks"),
            df_tasks_group["client_id"].min(),
            df_tasks_group["func_name"]
            .unique()
            .map(np.sort, meta=pd.Series(dtype=object))
            .str.join(",")
            .rename("func_names"),
        ],
        axis=1,
        ignore_unknown_divisions=True,
    )

    return df_graphs


def _func_name(label):
    return label.split("-#")[0].replace("(", "").replace("'", "")


def read_tasks(log_path):
    df_tasks = read_tasks_raw(log_path)
    unique_graph_ids = _get_unique_graph_ids(log_path + "/graph_*.dsk")
    key_id_mapping = _get_key_id_mapping(log_path, unique_graph_ids)

    df_tasks["id"] = df_tasks["key"].map(
        {str(key): value for key, value in key_id_mapping.items()}
    )
    df_tasks["duration"] = (df_tasks["stop"] - df_tasks["start"]).dt.total_seconds()

    return df_tasks


def read_versions(log_path, list_of_keys=None):
    """
    Read version information from log path.

    - Calculate number of available workers

    Parameters
    ----------
    log_path: str
        Path to read the logging files.
    list_of_keys: list of list of int,str
        Version information is nested. Provide a list of list of the keys to retrieve this information.

    Examples
    --------
    >>> # Logs must already exist for this to work. (n_workers not shown)
    >>> import dask_log_server
    >>> list_of_keys = [["scheduler", "host", "OS"], ["scheduler", "packages", "dask"], ["scheduler", "packages", "python"]]
    >>> dask_log_server.read_versions("logs", list_of_keys).compute()
                              datetime   status        client_id                                           versions scheduler-host-OS scheduler-packages-dask scheduler-packages-python
    0 2020-07-13 15:13:55.322711+00:00  running  139859169225248  {'scheduler': {'host': {'python': '3.6.9.final...             Linux                  2.20.0             3.6.9.final.0
    0 2020-07-08 17:18:28.451828+00:00  running  140103390383688  {'scheduler': {'host': {'python': '3.6.9.final...             Linux                  2.20.0             3.6.9.final.0

    Returns
    -------
    pandas.DataFrame

    """
    if list_of_keys is None:
        list_of_keys = []
    df_versions = dd.read_json(log_path + "/version*.json")
    for keys in list_of_keys:
        column_name = "-".join(keys)
        df_versions[column_name] = df_versions["versions"].map(
            functools.partial(_get_nested, keys=keys), meta=(column_name, str)
        )
    df_versions["n_workers"] = (
        dd.read_json("logs/info_*.jsonl")["info"]
        .map(
            functools.partial(_get_nested, keys=["workers"]), meta=("n_workers", object)
        )
        .map(len)
    )
    return df_versions


def _get_nested(dict_, keys):
    """
    Nested get method for dictionaries (and lists, tuples).
    """
    try:
        for key in keys:
            dict_ = dict_[key]
    except (KeyError, IndexError, TypeError):
        return None
    return dict_


def _get_unique_graph_ids(path):
    paths = glob.glob(path)
    paths_pool = set(paths)
    paths_unique_count = dict()
    while len(paths_pool):
        path = paths_pool.pop()
        paths_unique_count[path] = 1
        delete_set = set()
        for other_path in paths_pool:
            if filecmp.cmp(path, other_path):
                delete_set.add(other_path)
                paths_unique_count[path] += 1
        paths_pool -= delete_set
    return {
        path.split("_")[-1].split(".")[-2]: count
        for path, count in paths_unique_count.items()
    }


def _get_key_id_mapping(log_path, unique_graph_ids):
    key_to_id = (
        _read_dask_graphs(log_path, unique_graph_ids)
        .map(
            lambda graph_id_dask_graph: (
                graph_id_dask_graph[0],
                tuple(graph_id_dask_graph[1].keys()),
            ),
        )
        .map(_flatten_tuple, 1, [0])
        .flatten()
    )
    return dict(key_to_id.compute())


def _read_dask_graphs(log_path, unique_graph_ids):
    """Return a list of tuple with (graph_id, dask_graph)"""
    dask_graphs = db.from_sequence(unique_graph_ids).map(
        lambda graph_id: (
            graph_id,
            distributed.protocol.deserialize(
                *pickle.load(open(log_path + "/graph_" + graph_id + ".dsk", "rb"))
            ),
        )
    )
    return dask_graphs


def get_actual_concurrency(df_tasks):
    """
    Calculate worker concurrency of task stream.

    Note: concurrency_sum should have two data points per timestamp with a
    vertical line connecting them. This is not yet implemented.

    Another Note: Melt puts value columns after one another.
    """
    df_concurrency = (
        df_tasks[(df_tasks["action"] == "compute")]
        .melt(
            id_vars=["client_id"], value_vars=["start", "stop"], value_name="datetime"
        )
        .set_index("datetime")
    )
    df_concurrency["concurrency"] = df_concurrency["variable"].map(
        {"start": 1, "stop": -1}
    )
    df_concurrency["concurrency_sum"] = df_concurrency.groupby("client_id")[
        "concurrency"
    ].cumsum()
    return df_concurrency


def get_max_concurrency(log_path, unique_graph_ids):
    """Get maximum theoretical concurrency of task graphs."""
    return (
        _read_dask_graphs(log_path, unique_graph_ids)
        .map(lambda id_graph: (id_graph[0], to_nx(id_graph[1])))
        .map(lambda id_graph: (id_graph[0], max_dag_concurrency(id_graph[1])))
        .to_dataframe({"graph_id": str, "max_concurrency": int})
        .set_index("graph_id", npartitions="auto")["max_concurrency"]
    )


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


def _flatten_tuple(nested_tuple, tuple_index, single_indices):
    entries = list(nested_tuple[tuple_index])
    single_data = [nested_tuple[single_index] for single_index in single_indices]
    return [[entry] + single_data for entry in entries]


def _to_tuple(string):
    try:
        return ast.literal_eval(string)
    except:
        return string


def _flatten_dict(nested_dict, list_key, single_keys):
    list_of_dict = nested_dict[list_key]
    column_data = {single_key: nested_dict[single_key] for single_key in single_keys}
    [dict_.update(column_data) for dict_ in list_of_dict]
    return list_of_dict


def _strip_instances(iterable, excluded_instances=None):
    """

    Parameters
    ----------
    iterable: list, dict, tuple
        Iterable (in most cases a dask task graph).
    excluded_instances:
        Names of excluded types, which will not be stripped. The default is None.

    Returns
    -------
    list, dict, tuple
        Iterable only with built-in types.

    """
    if excluded_instances is None:
        excluded_instances = list()

    if isinstance(iterable, list):
        stripped_iterable = list()
        for item in iterable:
            stripped_iterable.append(_strip_instances(item, excluded_instances))
        return stripped_iterable

    elif isinstance(iterable, tuple):
        stripped_iterable = list()
        for item in iterable:
            stripped_iterable.append(_strip_instances(item, excluded_instances))
        return tuple(stripped_iterable)

    elif isinstance(iterable, dict):
        stripped_iterable = dict()
        for key, value in iterable.items():
            stripped_iterable[key] = _strip_instances(value, excluded_instances)
        return stripped_iterable
    elif isinstance(iterable, (int, bool, float, str)) or iterable is None:
        return iterable
    else:
        try:
            full_name = iterable.__module__ + "." + iterable.__name__
        except:
            full_name = (
                iterable.__class__.__module__ + "." + iterable.__class__.__name__
            )
        if full_name in excluded_instances:
            return iterable
        else:
            return callable


def visualize(dsk, df_tasks, label="", color="", current_time=0, **kwargs):
    """
    Draw a dask graph enhanced by additional information.

    Parameters
    ----------
    dsk: dict
        Dask task graph. Should be able to be plotted by dask.visualize.
    df_tasks: pd.DataFrame
        DataFrame of the dask task stream data. "key" column is mandatory to
        assign a row of the DataFrame to a node in the graph. "key" column
        must be of type string even when key is a tuple, because otherwise
        the type is not compatible with formats like parquet.
    label: str
        Column name of df_tasks DataFrame which contains the value for the
        node label.
    color: str
        Column name of df_tasks DataFrame which contains color information of
        node fill color.

        If the values are numerical the node is filled with grayscale tones.
        The label font color is adjusted to be always readable.

        If the values are strings each unique value is assigned a different
        color.

        If the value is "progress" each started node is filled with red and
        each finished is filled with blue. To set the current time use the
        argument "current_time". The option needs the columns "start_delta"
        and "stop_delta" in the df_tasks DataFrame containing the seconds
        passed since the start of the graph execution.
    current_time: float
        If color is set to "progress" this sets the current time influencing
        the fill color of the nodes.
    """
    attributes = _get_dsk_attributes(
        dsk, df_tasks, label_col=label, color_col=color, current_time=current_time
    )

    return dask.visualize(
        dsk,
        data_attributes=attributes["data"],
        function_attributes=attributes["func"],
        **kwargs,
    )


def _get_dsk_attributes(dsk, df_tasks, label_col="", color_col="", current_time=0.0):
    """
    See visualize for doc string.
    """
    # Filter unnecessary tasks, dtype of key is str
    df_tasks = df_tasks[df_tasks["key"].isin([str(key) for key in dsk.keys()])]
    if color_col == "progress" or color_col == "":
        color_type = "progress"
    elif pd.api.types.is_numeric_dtype(df_tasks[color_col]):
        color_type = "float"
        max_color_value = df_tasks[color_col].max()
    elif pd.api.types.is_string_dtype(
        df_tasks[color_col]
    ) or pd.api.types.is_categorical_dtype(df_tasks[color_col]):
        color_type = "category"
        random.seed(10)
        unique_colors = {
            value: f"#{random.randint(0, 0xFFFFFF):06X}"
            for value in df_tasks[color_col].unique()
        }
    else:
        raise ValueError("Could not get type on how to color graph.")

    attributes = {"func": dict(), "data": dict()}
    for index, df_single_task in df_tasks.iterrows():
        if df_single_task["action"] == "compute":
            attribute_name = "func"
        else:
            attribute_name = "data"
        key = df_single_task["key"]
        attributes[attribute_name][key] = {}
        if label_col != "":
            label = df_single_task[label_col]
            if not pd.isna(label):
                attributes[attribute_name][key]["label"] = str(label)
        if color_col == "progress":
            if df_single_task["stop_delta"] < current_time:
                attributes[attribute_name][key]["style"] = "filled"
                attributes[attribute_name][key]["fillcolor"] = "blue"
            elif df_single_task["start_delta"] < current_time:
                attributes[attribute_name][key]["style"] = "filled"
                attributes[attribute_name][key]["fillcolor"] = "red"
        elif color_type == "float":
            try:
                grayscale = 100 - int(df_single_task[color_col] / max_color_value * 100)
            except ValueError:
                continue
            attributes[attribute_name][key]["style"] = "filled"
            attributes[attribute_name][key]["fillcolor"] = f"gray{grayscale}"
            if grayscale < 20:
                attributes[attribute_name][key]["fontcolor"] = "white"
        elif color_type == "category":
            attributes[attribute_name][key]["style"] = "filled"
            attributes[attribute_name][key]["fillcolor"] = unique_colors[
                df_single_task[color_col]
            ]

    attributes["data"] = {
        _to_tuple(key): value for key, value in attributes["data"].items()
    }
    attributes["func"] = {
        _to_tuple(key): value for key, value in attributes["func"].items()
    }
    return attributes
