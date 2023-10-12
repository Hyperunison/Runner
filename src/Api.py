import json
import logging
from typing import List, Dict

from src.auto.auto_api_client.api.agent_api import AgentApi
from auto_api_client.model.add_run_log_chunk_request import AddRunLogChunkRequest

from src.auto.auto_api_client.model.mapping_resolve_response import MappingResolveResponse
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from auto_api_client.model.set_process_logs_request import SetProcessLogsRequest

from auto_api_client.model.set_cohort_definition_aggregation_request import SetCohortDefinitionAggregationRequest
from auto_api_client.model.get_mappings_request import GetMappingsRequest

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

    def set_cohort_definition_aggregation(self, data: any, sql: str, channel: str, key: str, raw_only: bool):
        self.api_instance.set_cohort_definition_aggregation(
            set_cohort_definition_aggregation_request=SetCohortDefinitionAggregationRequest(
                result=json.dumps(data),
                sql=sql,
            ),
            channel=channel,
            version=self.version,
            token=self.token,
            key=key,
            raw_only="1" if raw_only else "0"
        )

    def add_log_chunk(self, id: int, chunk: str):
        logging.info("Adding logs to run={}, len={}".format(id, len(chunk)))
        try:
            self.api_instance.add_run_log_chunk(id=id, add_run_log_chunk_request=AddRunLogChunkRequest(chunk=chunk), token=self.token, version=self.version)
        except Exception as e:
            logging.error("Cannot add logs to run={}, len={}".format(id, str(e)))

    def accept_task(self, id: int):
        logging.info("Accepting task id={}".format(id))
        self.api_instance.accept_task(id=str(id), token=self.token, version=self.version)

    def resolve_mapping(self, key: str, request: Dict[str, List[str]]) -> List[MappingResolveResponse]:
        logging.info("Resolving mapping for key={}, request={}".format(key, json.dumps(request)))
        res = self.api_instance.get_mappings(token=self.token, version=self.version, key=key, get_mappings_request=GetMappingsRequest(input=json.dumps(request)))
        return res
    def block_task(self, id: int, runner_instance: str):
        logging.info("Blocking task id={}".format(id))
        return self.api_instance.block_task(id=str(id), token=self.token, version=self.version, runner_instance=runner_instance)

    def get_agent_id(self):
        return self.api_instance.get_agent_id(token=self.token, version=self.version)
