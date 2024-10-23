import os
from src.Message.StartWorkflow import StartWorkflow
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.UCDM.DataSchema import DataSchema


class WorkflowBase:
    mapping_file_name: str = "var/mapping-values.csv"
    api: Api
    adapter: BaseAdapter
    schema: DataSchema

    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema):
        self.api = api
        self.adapter = adapter
        self.schema = schema

    def execute(self, message: StartWorkflow):
        pass

    def download_mapping(self):
        response = self.api.export_mapping()
        with open(os.path.abspath(self.mapping_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)