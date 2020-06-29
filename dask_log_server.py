import threading
import time
import datetime
import json
import pytz
import pathlib
import uuid
import pickle
import distributed


def graph_logger_config(get, log_path):
    def graph_logger(*args, **kwargs):
        with open(f"{log_path}graphs_{uuid.uuid4().hex[:16]}.dsk", "wb") as file:
            # TODO: Add case when only key word arguments are given
            file.write(pickle.dumps(distributed.protocol.serialize(args[0])))
        return get(*args, **kwargs)
    return graph_logger


def dask_logger_config(time_interval=60, log_path="logs/", n_tasks_min=1, filemode="a"):
    """
    Configure the dask logger to your liking.

    Parameters
    ----------
    time_interval: int
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

        def task_logger():
            thread = threading.currentThread()
            last_time = time.time()
            unique_id = uuid.uuid4().hex[:16]
            while getattr(thread, "do_run", True):
                if dask_client.status == "running":
                    now_time = time.time()
                    tasks = dask_client.get_task_stream(last_time, now_time)
                    if (len(tasks) >= n_tasks_min) or getattr(thread, "force_log", False) and (len(tasks) >= 1):
                        thread.force_log = False
                        last_time = now_time
                        [task.pop("type") for task in tasks]
                        log_message = {
                            "datetime": str(datetime.datetime.now(pytz.utc)),
                            "status": dask_client.status,
                            "client_id": str(id(dask_client)),
                            "tasks": tasks
                        }
                        if filemode == "w":
                            unique_id = uuid.uuid4().hex[:16]
                        with open(f"{log_path}tasks_{unique_id}.jsonl", filemode) as file:
                            file.write(json.dumps(log_message))
                            file.write("\n")
                time.sleep(time_interval)
        dask_client.task_logger = threading.Thread(target=task_logger)
        dask_client.task_logger.do_run = True
        dask_client.task_logger.force_log = False
        dask_client.task_logger.start()

        dask_client.get = graph_logger_config(dask_client.get, log_path=log_path)

        return dask_client
    return dask_logger
