import logging
from src.Api import Api

class ApiLogger:
    api: Api

    def __init__(self, api: Api):
        self.api = api

    def write(self, runner_message_id: int, line: str):
        logging.info(line)
        try:
            self.api.add_job_logs(runner_message_id, line)
        except Exception as e:
            pass
