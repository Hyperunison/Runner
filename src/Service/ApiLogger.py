import logging
from src.Api import Api
from datetime import datetime

class ApiLogger:
    api: Api

    def __init__(self, api: Api):
        self.api = api

    def write(self, runner_message_id: int, message: str):
        time: str = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
        logging.info(message)
        logging.info("Sending logs to {}".format(runner_message_id))
        line = "[{}] {}".format(time, message)
        try:
            self.api.add_job_logs(runner_message_id, line)
        except Exception as e:
            pass
