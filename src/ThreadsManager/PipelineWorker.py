from typing import Dict
import logging
import os
import socket

from src.auto.auto_api_client.api import agent_api
from src.Api import Api
from src.Service.Workflows.VendorPipelines import VendorPipelines
from src.UCDM.DataSchema import DataSchema
from src.Adapters.AdapterFactory import create_by_config
from src.auto.auto_api_client.api_client import ApiClient
from src.auto.auto_api_client.configuration import Configuration
from src.Service.Workflows.NextflowCohortWorkflowExecutor import NextflowCohortWorkflowExecutor

class PipelineWorker:
    config: Dict
    configuration: Configuration = None

    def __init__(
            self,
            config: Dict,
            configuration: Configuration
    ):
        self.config = config
        self.configuration = configuration

    def write_log_info(self, message: str):
        logging.info("PipelineWorker: " + message)

    def write_log_critical(self, message: str):
        logging.critical("PipelineWorker: " + message)

    def run(self):
        self.write_log_info("Starting PipelineWorker")

        with ApiClient(self.configuration) as api_client:
            runner_instance_id = socket.gethostname() + "-" + str(os.getpid())
            api_instance = agent_api.AgentApi(api_client)
            api = Api(api_instance, self.config['api_version'], self.config['agent_token'], self.config['api_request_cookie'])
            pipeline_executor = create_by_config(api, self.config, runner_instance_id)
            schema = DataSchema(self.config['phenotypic_db']['dsn'], self.config['phenotypic_db']['min_count'])
            vendor_pipelines = VendorPipelines(api, pipeline_executor, schema)
            workflow_executor = NextflowCohortWorkflowExecutor(api, pipeline_executor, schema, vendor_pipelines)
            vendor_pipelines.sync_pipeline_list_with_backend()