import logging
from src.Api import Api

class ApiLogger:
    api: Api

    def __init__(self, api: Api):
        self.api = api

    def write(self, job_id: int, line: str):
        logging.info(line)
        self.api.add_job_logs(job_id, line)
