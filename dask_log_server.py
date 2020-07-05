import threading
import time
import datetime
import json
import pytz
import pathlib
import uuid
import pickle
import base64

import distributed
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


def read_tasks(urlpath):
    """
    Read tasks from one or multiple task log files.

    """
    df_tasks = (
        db.read_text(urlpath)
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
                "action": "string",
                "start": "float64",
                "stop": "float64",
                "worker": "string",
                "status": "string",
                "nbytes": "int64",
                "thread": "int64",
                "type": "string",
                "typename": "string",
                "key": "string",
                "datetime": "string",
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
