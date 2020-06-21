import threading
import time
import datetime
import json
import pytz
import pathlib


def dask_logger_config(time_interval=60, log_path="logs/", n_tasks_min=1):
    def dask_logger(dask_client):
        pathlib.Path(log_path).mkdir(parents=True, exist_ok=True)

        def logger():
            thread = threading.currentThread()
            last_time = time.time()
            while getattr(thread, "do_run", True):
                if dask_client.status == "running":
                    now_time = time.time()
                    tasks = dask_client.get_task_stream(last_time, now_time)
                    if len(tasks) >= n_tasks_min:
                        last_time = now_time
                        [task.pop("type") for task in tasks]
                        log_message = {
                            "datetime": str(datetime.datetime.now(pytz.utc)),
                            "status": dask_client.status,
                            "client_id": str(id(dask_client)),
                            "tasks": tasks
                        }
                        with open(f"{log_path}/logger_{datetime.datetime.now()}.jsonl", "a") as file:
                            file.write(json.dumps(log_message))
                            file.write("\n")
                time.sleep(time_interval)
        dask_client.logger = threading.Thread(target=logger)
        dask_client.logger.do_run = True
        dask_client.logger.start()
        return dask_client
    return dask_logger
