from typing import Dict
import logging
import os
import socket
import threading

import auto_api_client

from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.Service.Workflows.NextflowCohortWorkflowExecutor import NextflowCohortWorkflowExecutor
from src.Service.Workflows.VendorPipelines import VendorPipelines
from src.UCDM.DataSchema import DataSchema
from src.auto.auto_api_client.api import agent_api
from src.Api import Api
from src.Adapters.AdapterFactory import create_by_config
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.Message.KillCohortAPIRequest import KillCohortAPIRequest
from src.Message.KillJob import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun
from src.Message.StartWorkflow import StartWorkflow
from src.Message.UpdateTablesList import UpdateTablesList
from src.Message.UpdateTableColumnsList import UpdateTableColumnsList
from src.Message.UpdateTableColumnStats import UpdateTableColumnStats
from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Service.MessageFactory import MessageFactory
from src.auto.auto_api_client.api_client import ApiClient

class Worker:

    config: Dict
    native_id: int = None
    manager: ConsoleApplicationManager = None

    def __init__(self, queue: str, config: Dict, manager: ConsoleApplicationManager):
        self.queue = queue
        self.config = config
        self.manager = manager

    def write_log_info(self, message: str):
        logging.info("Process {}: ".format(self.native_id) + message)

    def write_log_critical(self, message: str):
        logging.critical("Process {}: ".format(self.native_id) + message)

    def run(self):
        self.native_id = threading.get_native_id()
        allow_private_upload_data_to_unison = self.config['allow_private_upload_data_to_unison'] == 1
        configuration = self.manager.initialize(self.config)

        with ApiClient(configuration) as api_client:
            try:
                runner_instance_id = socket.gethostname() + "-" + str(os.getpid())
                api_instance = agent_api.AgentApi(api_client)
                api = Api(api_instance, self.config['api_version'], self.config['agent_token'])
                pipeline_executor = create_by_config(api, self.config, runner_instance_id)
                schema = DataSchema(self.config['phenotypic_db']['dsn'], self.config['phenotypic_db']['min_count'])
                vendor_pipelines = VendorPipelines(api, pipeline_executor, schema)
                workflow_executor = NextflowCohortWorkflowExecutor(api, pipeline_executor, schema, vendor_pipelines)

                vendor_pipelines.sync_pipeline_list_with_backend()

                response = api.next_task()
                message = MessageFactory().create_message_object_from_response(message=response)
                if not message is None:
                    self.write_log_info("Received message {}".format(response.type))

                if type(message) is NextflowRun:
                    pipeline_executor.process_nextflow_run(message)
                elif type(message) is GetProcessLogs:
                    pipeline_executor.adapter.process_get_process_logs(message)
                elif type(message) is KillJob:
                    pipeline_executor.adapter.process_kill_job(message)
                elif type(message) is CohortAPIRequest:
                    schema.execute_cohort_definition(message, api)
                elif type(message) is KillCohortAPIRequest:
                    schema.kill_cohort_definition(message, api)
                elif type(message) is UpdateTablesList:
                    schema.update_tables_list(
                        api,
                        self.config['data_protected']['schemas'],
                        self.config['data_protected']['tables']
                    )
                elif type(message) is UpdateTableColumnsList:
                    schema.update_table_columns_list(
                        api,
                        message,
                        self.config['data_protected']['columns']
                    )
                elif type(message) is UpdateTableColumnStats:
                    schema.update_table_column_stats(
                        api,
                        message,
                        self.config['phenotypic_db']['min_count'],
                        self.config['data_protected']['tables'],
                        self.config['data_protected']['columns']
                    )
                elif type(message) is StartWorkflow:
                    workflow_executor.execute_workflow(message, allow_private_upload_data_to_unison)
                elif type(message) is StartOMOPoficationWorkflow:
                    workflow_executor.execute_workflow(message, allow_private_upload_data_to_unison)
                elif message is None:
                    pass
                else:
                    error = "Unknown message type {}".format(type(message))
                    if not response is None and not response.id is None:
                        api.set_task_error(response.id, error)
            except auto_api_client.ApiException as e:
                error = "Exception when calling AgentApi: %s\n" % e
                self.write_log_critical(error)
                if not response is None and not response.id is None:
                    api.set_task_error(response.id, error)

            except Exception as e:
                error = "Unknown exception: %s\n" % e
                self.write_log_critical(error)
                if not response is None and not response.id is None:
                    api.set_task_error(response.id, error)