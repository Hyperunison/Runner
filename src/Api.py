import json
import logging
from typing import List, Dict, Optional

from src.auto.auto_api_client.api.agent_api import AgentApi
from auto_api_client.model.add_run_log_chunk_request import AddRunLogChunkRequest

from src.auto.auto_api_client.model.mapping_resolve_response import MappingResolveResponse
from auto_api_client.model.post_app_agent_addjoblogs_request import PostAppAgentAddjoblogsRequest
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from auto_api_client.model.set_process_logs_request import SetProcessLogsRequest

from auto_api_client.model.set_cohort_definition_aggregation_request import SetCohortDefinitionAggregationRequest
from auto_api_client.model.get_mappings_request import GetMappingsRequest

from auto_api_client.model.set_tables_list_request import SetTablesListRequest

from auto_api_client.model.set_table_stats_request import SetTableStatsRequest

from auto_api_client.model.set_table_column_stats_request import SetTableColumnStatsRequest

from auto_api_client.model.set_table_column_freequent_values_request import SetTableColumnFreequentValuesRequest
from auto_api_client.model.set_table_info_request import SetTableInfoRequest

from auto_api_client.model.set_job_state_request import SetJobStateRequest

from auto_api_client.model.set_task_error_request import SetTaskErrorRequest

from auto_api_client.model.set_sql_query_for_cohort_api_request_request import \
    SetSQLQueryForCohortApiRequestRequest

from auto_api_client.model.set_run_dir_request import SetRunDirRequest

from auto_api_client.model.set_installed_pipelines_request import SetInstalledPipelinesRequest


class Api:
    api_instance: AgentApi = None
    version: str = None
    token: str = None

    def __init__(self, api_instance: AgentApi, version: str, token: str):
        self.api_instance = api_instance
        self.version = str(version)
        self.token = str(token)

    def next_task(self, queues: List[str]) -> RunnerMessage:
        return self.api_instance.get_next_task(self.version, self.token, queues=queues)

    def set_run_status(self, id: int, status: str):
        logging.info("Change status of run={} to {}".format(id, status))
        self.api_instance.set_run_status(id=id, status=status, version=self.version, token=self.token)

    def set_run_dir(self, id: int, dir: str):
        logging.info("Change dir of run={} to {}".format(id, dir))
        self.api_instance.set_run_dir(id=id, set_run_dir_request=SetRunDirRequest(dir=dir), version=self.version, token=self.token)

    def set_process_logs(self, process_id: str, logs: str, channel: str):
        self.api_instance.set_process_logs(
            process_id=process_id,
            version=self.version,
            channel=channel,
            token=self.token,
            set_process_logs_request=SetProcessLogsRequest(logs=logs)
        )

    def add_job_logs(self, id: int, logs: str):
        self.api_instance.post_app_agent_addjoblogs(
            version=self.version,
            token=self.token,
            run_id=str(id),
            post_app_agent_addjoblogs_request=PostAppAgentAddjoblogsRequest(logs=logs)
        )

    def set_kill_result(self, run_id: int, channel: str):
        self.api_instance.set_kill_result(
            id=int(run_id),
            channel=channel,
            token=self.token,
            version=self.version
        )

    def set_cohort_definition_aggregation(
            self,
            data: any,
            sql: str,
            channel: str,
            key: str,
            raw_only: bool
    ):
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

    def set_job_state(self, run_id: str, state: str, percent: int, path: str, dqd_path: Optional[str] = None):
        self.api_instance.set_job_state(
            run_id=run_id,
            version=self.version,
            token=self.token,
            set_job_state_request=SetJobStateRequest(
                state=state,
                result=json.dumps({
                    "percent": percent,
                    "path": path,
                    "dqdPath": dqd_path,
                })
            )
        )

    def set_cohort_sql_query(self, id: int, sql: str):
        self.api_instance.set_sql_query_for_cohort_api_request(
            id=str(id),
            version=self.version,
            token=self.token,
            set_sql_query_for_cohort_api_request_request=SetSQLQueryForCohortApiRequestRequest(
                sql=sql
            )
        )

    def set_cohort_error(self, id: int, error: str):
        self.api_instance.set_error_for_cohort_api_request(
            id=str(id),
            version=self.version,
            token=self.token,
            set_task_error_request=SetTaskErrorRequest(
                error=error
            )
        )

    def export_mapping(self, cdm_id: int):
        logging.info("Export mapping to response body file")

        return self.api_instance.export_mapping_by_cdm(
            version=self.version,
            token=self.token,
            cdm_id=str(cdm_id)
        )

    def export_cdm_concept(self, cdm_id: str):
        logging.info("Export CDM concepts to response body file")

        return self.api_instance.export_cdm_values_csv(
            version=self.version,
            token=self.token,
            cdm_id=cdm_id
        )

    def export_cdm_vocabulary(self, cdm_id: str):
        logging.info("Export CDM vocabulary to response body file")

        return self.api_instance.export_cdm_vocabulary_csv(
            version=self.version,
            token=self.token,
            cdm_id=cdm_id
        )

    def export_mapping_docs(self):
        logging.info("Export mapping docs")

        return self.api_instance.export_docs_for_specific_biobank(
            version=self.version,
            token=self.token
        )

    def export_mapping_docs_csv(self):
        logging.info("Export mapping docs csv")

        return self.api_instance.export_docs_for_specific_biobank_in_csv_format(
            version=self.version,
            token=self.token
        )

    def send_installed_pipelines(self, pipelines: List[str]):
        logging.debug("Sending list of installed pipelines: {}".format(", ".join(pipelines)))
        result = self.api_instance.set_installed_pipelines(
            version=self.version,
            token=self.token,
            set_installed_pipelines_request=SetInstalledPipelinesRequest(pipelines=pipelines)
        )
        if result != 'OK':
            logging.error("Error with sending list of pipelines to server")

    def add_log_chunk(self, id: int, chunk: str):
        logging.info("Adding logs to run={}, len={}".format(id, len(chunk)))
        try:
            self.api_instance.add_run_log_chunk(id=id, add_run_log_chunk_request=AddRunLogChunkRequest(chunk=chunk), token=self.token, version=self.version)
        except Exception as e:
            logging.error("Cannot add logs to run={}, len={}".format(id, str(e)))

    def accept_task(self, id: int):
        logging.info("Accepting task id={}".format(id))
        self.api_instance.accept_task(id=str(id), token=self.token, version=self.version)

    def set_task_error(self, id: int, error: str):
        logging.info("Setting task id={} to error".format(id))
        logging.debug("Error of task id={} is {}".format(id, error))
        self.api_instance.set_task_error(id=str(id), token=self.token, version=self.version, set_task_error_request=SetTaskErrorRequest(error=error))

    def set_tables_list(self, tables: List[str]):
        logging.info("Sending list tables {}".format(', '.join(tables)))
        self.api_instance.set_tables_list(set_tables_list_request=SetTablesListRequest(tables=tables), token=self.token, version=self.version)

    def set_table_stats(self, table_name: str, rows_count: int, columns: List[str], types: List[str], nullable_result: List[str]):
        logging.info("Sending table stats for table {}. rows_count={}, columns: ".format(table_name, rows_count, ', '.join(columns)))
        self.api_instance.set_table_stats(
            set_table_stats_request=SetTableStatsRequest(
                rows_count=str(rows_count),
                columns=columns,
                nullable=nullable_result,
                types=types,
            ),
            table=table_name,
            token=self.token,
            version=self.version
        )



    def set_table_info(self, table_name: str, abandoned: bool):
        logging.info("Sending table info for table {}".format(table_name))
        self.api_instance.set_table_info(
            table=table_name,
            version=self.version,
            token=self.token,
            set_table_info_request=SetTableInfoRequest(
                abandoned='1' if abandoned == True else '0'
            )
        )

    def set_table_column_stats(
            self, table_name: str, column_name: str, unique_count, nulls_count,
            min_value, max_value, avg_value, median12_value, median25_value, median37_value,
            median50_value, median63_value, median75_value, median88_value,
    ):
        logging.info("Sending table column stats for table {}.{}".format(table_name, column_name))
        self.api_instance.set_table_column_stats(
            set_table_column_stats_request=SetTableColumnStatsRequest(
                is_private='0',
                unique_count=str(unique_count) if not unique_count is None else '',
                nulls_count=str(nulls_count) if not nulls_count is None else '',
                min_value=str(min_value) if not min_value is None else '',
                max_value=str(max_value) if not max_value is None else '',
                avg_value=str(avg_value) if not avg_value is None else '',
                median12_value=str(median12_value) if not median12_value is None else '',
                median25_value=str(median25_value) if not median25_value is None else '',
                median37_value=str(median37_value) if not median37_value is None else '',
                median50_value=str(median50_value) if not median50_value is None else '',
                median63_value=str(median63_value) if not median63_value is None else '',
                median75_value=str(median75_value) if not median75_value is None else '',
                median88_value=str(median88_value) if not median88_value is None else '',
            ),
            table=table_name,
            column=column_name,
            token=self.token,
            version=self.version
        )

    def set_table_column_values(self, table_name: str, column_name: str, values: List[str], counts: List[int]):
        res = self.api_instance.set_table_column_freequent_values(
            table=table_name,
            column=column_name,
            token=self.token,
            version=self.version,
            set_table_column_freequent_values_request=SetTableColumnFreequentValuesRequest(
                values=values,
                counts=counts,
            )
        )

        return res

    def resolve_mapping(self, key: str, request: Dict[str, List[str]]) -> List[MappingResolveResponse]:
        logging.info("Resolving mapping for key={}, request={}".format(key, json.dumps(request)))
        res = self.api_instance.get_mappings(token=self.token, version=self.version, key=key, get_mappings_request=GetMappingsRequest(input=json.dumps(request)))
        return res

    def block_task(self, id: int, runner_instance: str):
        logging.info("Blocking task id={}".format(id))
        return self.api_instance.block_task(id=str(id), token=self.token, version=self.version, runner_instance=runner_instance)

    def get_agent_id(self):
        return self.api_instance.get_agent_id(token=self.token, version=self.version)

    def set_car_status(self, car_id: int, status: str, pid: int = None):
        logging.info("Update CAR {} status to {}, pid={}".format(int(car_id), status, pid))
        self.api_instance.set_car_status(int(car_id), status=status, pid=str(pid), token=self.token, version=self.version)