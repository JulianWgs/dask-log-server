import filecmp
import functools
import glob
import json
import pickle

import dask
import dask.bag as db
import dask.dataframe as dd
import dask.dot
import numpy as np
import pandas as pd

import distributed

from .helper import _flatten_dict, _flatten_tuple, _func_name, _get_nested


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


def read_tasks(log_path):
    df_tasks = read_tasks_raw(log_path)
    unique_graph_ids = _get_unique_graph_ids(log_path + "/graph_*.dsk")
    key_id_mapping = _get_key_id_mapping(log_path, unique_graph_ids)

    df_tasks["id"] = df_tasks["key"].map(
        {str(key): value for key, value in key_id_mapping.items()}
    )
    df_tasks["duration"] = (df_tasks["stop"] - df_tasks["start"]).dt.total_seconds()

    return df_tasks


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


def get_max_concurrency(log_path, unique_graph_ids):
    """Get maximum theoretical concurrency of task graphs."""
    # Import here, because of networkx dependency
    from .graph import to_nx, max_dag_concurrency

    return (
        _read_dask_graphs(log_path, unique_graph_ids)
        .map(lambda id_graph: (id_graph[0], to_nx(id_graph[1])))
        .map(lambda id_graph: (id_graph[0], max_dag_concurrency(id_graph[1])))
        .to_dataframe({"graph_id": str, "max_concurrency": int})
        .set_index("graph_id", npartitions="auto")["max_concurrency"]
    )
