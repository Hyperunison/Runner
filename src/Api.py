import json
import logging

from src.auto.auto_api_client.api.agent_api import AgentApi
from auto_api_client.model.add_run_log_chunk_request import AddRunLogChunkRequest
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from auto_api_client.model.set_process_logs_request import SetProcessLogsRequest

from auto_api_client.model.set_cohort_definition_aggregation_request import SetCohortDefinitionAggregationRequest

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

    def set_process_logs(self, process_id: str, logs: str, channel: str):
        self.api_instance.set_process_logs(
            process_id=process_id,
            version=self.version,
            channel=channel,
            token=self.token,
            set_process_logs_request=SetProcessLogsRequest(logs=logs)
        )

    def set_kill_result(self, run_id: int, channel: str):
        self.api_instance.set_kill_result(
            id=int(run_id),
            channel=channel,
            token=self.token,
            version=self.version
        )

    def set_cohort_definition_aggregation(self, data: any, sql: str, channel: str):
        self.api_instance.set_cohort_definition_aggregation(
            set_cohort_definition_aggregation_request=SetCohortDefinitionAggregationRequest(
                result=json.dumps(data),
                sql=sql
            ),
            channel=channel,
            version=self.version,
            token=self.token,
        )

    def add_log_chunk(self, id: int, chunk: str):
        logging.info("Adding logs to run={}, len={}".format(id, len(chunk)))
        self.api_instance.add_run_log_chunk(id=id, add_run_log_chunk_request=AddRunLogChunkRequest(chunk=chunk), token=self.token, version=self.version)

    def accept_task(self, id: int):
        logging.info("Accepting task id={}".format(id))
        self.api_instance.accept_task(id=str(id), token=self.token, version=self.version)