import threading
import time
import datetime
import json
import pytz
import pathlib
import uuid
import pickle
import base64
import random
import ast
import glob
import filecmp

import numpy as np
import pandas as pd
import distributed
import dask
import dask.dot
import dask.dataframe as dd
import dask.bag as db


def graph_logger_config(get, log_path):
    def graph_logger(*args, **kwargs):
        with open(f"{log_path}graph_{uuid.uuid4().hex[:16]}.dsk", "wb") as file:
            # TODO: Add case when only key word arguments are given
            dsk = _strip_instances(args[0])
            file.write(pickle.dumps(distributed.protocol.serialize(dsk)))
        return get(*args, **kwargs)

    return graph_logger


def dask_logger_config(
    time_interval=60.0, info_interval=1.0, log_path="logs/", n_tasks_min=1, filemode="a"
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

        def versions_logger():
            while dask_client.status != "running":
                time.sleep(1)
            log_message = {
                "datetime": str(datetime.datetime.now(pytz.utc)),
                "status": dask_client.status,
                "client_id": str(id(dask_client)),
                "versions": dask_client.get_versions(),
            }
            unique_id = uuid.uuid4().hex[:16]
            with open(f"{log_path}versions_{unique_id}.jsonl", "w") as file:
                file.write(json.dumps(log_message))
                file.write("\n")

        dask_client.versions_logger = threading.Thread(target=versions_logger)
        dask_client.versions_logger.start()

        def task_logger():
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
                            task.update(
                                {"type": base64.b64encode(task["type"]).decode()}
                            )
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
                        with open(
                            f"{log_path}tasks_{unique_id}.jsonl", filemode
                        ) as file:
                            file.write(json.dumps(log_message))
                            file.write("\n")
                time.sleep(time_interval)

        dask_client.task_logger = threading.Thread(target=task_logger)
        dask_client.task_logger.do_run = True
        dask_client.task_logger.force_log = False
        dask_client.task_logger.start()

        def info_logger():
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

        dask_client.info_logger = threading.Thread(target=info_logger)
        dask_client.info_logger.do_run = True
        dask_client.info_logger.start()

        dask_client.get = graph_logger_config(dask_client.get, log_path=log_path)

        return dask_client

    return dask_logger


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
    df_graph = dd.concat(
        [
            df_starts,
            df_stops,
            (df_stops - df_starts).rename("duration"),
            df_tasks_group["worker"].nunique().rename("n_workers"),
            df_tasks_group["status"].count().rename("n_tasks"),
            df_tasks_group["client_id"].min(),
            df_tasks_group["func_name"]
            .unique()
            .map(np.sort)
            .str.join(",")
            .rename("func_names"),
        ],
        axis=1,
        ignore_unknown_divisions=True,
    )

    return df_graph


def _func_name(label):
    return label.split("-#")[0].replace("(", "").replace("'", "")


def read_tasks(log_path):
    df_tasks = read_tasks_raw(log_path)
    unique_graph_ids = _get_unique_graph_ids(log_path + "/graph_*.dsk")
    key_id_mapping = _get_key_id_mapping(log_path, unique_graph_ids)

    df_tasks["id"] = df_tasks["key"].map(
        {str(key): value for key, value in key_id_mapping.items()}
    )
    # df_tasks = df_tasks.merge(df_versions[["client_id", "versions"]], on="client_id")
    df_tasks["duration"] = (df_tasks["stop"] - df_tasks["start"]).dt.total_seconds()

    return df_tasks


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
        db.from_sequence(unique_graph_ids)
        .map(
            lambda graph_id: (
                graph_id,
                tuple(
                    distributed.protocol.deserialize(
                        *pickle.load(
                            open(log_path + "/graph_" + graph_id + ".dsk", "rb")
                        )
                    ).keys()
                ),
            )
        )
        .map(_flatten_tuple, 1, [0])
        .flatten()
    )
    return dict(key_to_id.compute())


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


def visualize(dsk, df_tasks, label="", color="", current_time=0):
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
        dsk, df_tasks, label=label, color=color, current_time=current_time
    )

    return dask.visualize(
        dsk, data_attributes=attributes["data"], function_attributes=attributes["func"]
    )


def _get_dsk_attributes(dsk, df_tasks, label="", color="", current_time=0):
    """
    See visualize for doc string.
    """
    # Filter unnecessary tasks, dtype of key is str
    df_tasks = df_tasks[df_tasks["key"].isin([str(key) for key in dsk.keys()])]
    if color == "progress" or color == "":
        color_type = "progress"
    elif pd.api.types.is_numeric_dtype(df_tasks[color]):
        color_type = "float"
        max_color_value = df_tasks[color].max()
    elif pd.api.types.is_string_dtype(df_tasks[color]):
        color_type = "category"
        random.seed(10)
        unique_colors = {
            value: f"#{random.randint(0, 0xFFFFFF):06X}"
            for value in df_tasks[color].unique()
        }

    attributes = {"func": dict(), "data": dict()}
    for index, df_single_task in df_tasks.iterrows():
        if df_single_task["action"] == "compute":
            attribute_name = "func"
        else:
            attribute_name = "data"
        key = df_single_task["key"]
        attributes[attribute_name][key] = {}
        if label != "":
            attributes[attribute_name][key]["label"] = str(df_single_task[label])
        if color == "progress":
            if df_single_task["stop_delta"] < current_time:
                attributes[attribute_name][key]["style"] = "filled"
                attributes[attribute_name][key]["fillcolor"] = "blue"
            elif df_single_task["start_delta"] < current_time:
                attributes[attribute_name][key]["style"] = "filled"
                attributes[attribute_name][key]["fillcolor"] = "red"
        elif color_type == "float":
            attributes[attribute_name][key]["style"] = "filled"
            grayscale = 100 - int(df_single_task[color] / max_color_value * 100)
            attributes[attribute_name][key]["fillcolor"] = f"gray{grayscale}"
            if grayscale < 20:
                attributes[attribute_name][key]["fontcolor"] = "white"
        elif color_type == "category":
            attributes[attribute_name][key]["style"] = "filled"
            attributes[attribute_name][key]["fillcolor"] = unique_colors[
                df_single_task[color]
            ]

    attributes["data"] = {
        _to_tuple(key): value for key, value in attributes["data"].items()
    }
    attributes["func"] = {
        _to_tuple(key): value for key, value in attributes["func"].items()
    }
    return attributes
