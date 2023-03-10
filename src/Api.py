import logging

from src.auto.auto_api_client.api.agent_api import AgentApi
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from auto_api_client.model.add_run_log_chunk_request import AddRunLogChunkRequest


class Api:
    api_instance: AgentApi = None
    version: str = None
    token: str = None

    def __init__(self, api_instance: AgentApi, version: str, token: str):
        self.api_instance = api_instance
        self.version = str(version)
        self.token = str(token)

    def next_task(self) -> RunnerMessage:
        return self.api_instance.get_next_task(self.version, self.token)

    def set_run_status(self, id: int, status: str):
        logging.info("Change status of run={} to {}".format(id, status))
        self.api_instance.set_run_status(id=id, status=status, version=self.version, token=self.token)

    def add_log_chunk(self, id: int, chunk: str):
        logging.info("Adding logs to run={}, len={}".format(id, len(chunk)))
        print(chunk);
        self.api_instance.add_run_log_chunk(id=id, add_run_log_chunk_request=AddRunLogChunkRequest(chunk=chunk), token=self.token, version=self.version)