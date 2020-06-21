import threading
import time
import datetime
import json
import pytz


def dask_logger(dask_client):
    def logger():
        thread = threading.currentThread()
        last_time = time.time()
        while getattr(thread, "do_run", True):
            if dask_client.status == "running":
                now_time = time.time()
                tasks = dask_client.get_task_stream(last_time, now_time)
                last_time = now_time
                if tasks:
                    [task.pop("type") for task in tasks]
                    log_message = {
                        "datetime": str(datetime.datetime.now(pytz.utc)),
                        "status": dask_client.status,
                        "client_id": str(id(dask_client)),
                        "tasks": tasks
                    }
                    with open(f"logs/logger_{datetime.datetime.now()}.jsonl", "a") as file:
                        file.write(json.dumps(log_message))
                        file.write("\n")
            time.sleep(5)
    dask_client.logger = threading.Thread(target=logger)
    dask_client.logger.do_run = True
    dask_client.logger.start()
    return dask_client
