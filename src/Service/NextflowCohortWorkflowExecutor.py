from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message.StartWorkflow import StartWorkflow
from src.Service.Workflows.GwasFederated.GwasFederated import GwasFederated
from src.Service.Workflows.OMOPification.OMOPofication import OMOPofication
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.UCDM.DataSchema import DataSchema


class NextflowCohortWorkflowExecutor:
    api: Api
    adapter: BaseAdapter
    schema: DataSchema
    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema):
        self.api = api
        self.adapter = adapter
        self.schema = schema

    def execute_workflow(self, message: StartWorkflow, may_upload_private_data: bool):
        workflow: WorkflowBase
        if message.workflow_name == 'GwasFederated':
            workflow = GwasFederated(self.api, self.adapter, self.schema)
        elif message.workflow_name == 'OMOPification':
            workflow = OMOPofication(self.api, self.adapter, self.schema, may_upload_private_data)
        else:
            raise ValueError("Unknown workflow {}".format(message.workflow_name))

        workflow.execute(message)