import os
from src.Message.StartWorkflow import StartWorkflow
from src.Api import Api
from src.Service import PipelineExecutor
from src.UCDM.DataSchema import DataSchema


class WorkflowBase:
    mapping_file_name: str = "var/mapping-values.csv"
    api: Api
    pipeline_executor: PipelineExecutor
    schema: DataSchema

    def __init__(self, api: Api, pipeline_executor: PipelineExecutor, schema: DataSchema):
        self.api = api
        self.pipeline_executor = pipeline_executor
        self.schema = schema

    def execute(self, message: StartWorkflow, api: Api):
        pass

    def download_mapping(self):
        response = self.api.export_mapping()
        with open(os.path.abspath(self.mapping_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)