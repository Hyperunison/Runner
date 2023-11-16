from src.Message.StartWorkflow import StartWorkflow
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.UCDM.DataSchema import DataSchema


class WorkflowBase:
    api: Api
    adapter: BaseAdapter
    schema: DataSchema

    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema):
        self.api = api
        self.adapter = adapter
        self.schema = schema

    def execute(self, message: StartWorkflow):
        pass
