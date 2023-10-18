from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message.StartWorkflow import StartWorkflow
from src.Service.Workflows.GwasFederated.GwasFederated import GwasFederated
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.UCDM.Schema.BaseSchema import BaseSchema


class NextflowCohortWorkflowExecutor:
    api: Api
    adapter: BaseAdapter
    schema: BaseSchema
    def __init__(self, api: Api, adapter: BaseAdapter, schema: BaseSchema):
        self.api = api
        self.adapter = adapter
        self.schema = schema

    def execute_workflow(self, message: StartWorkflow):
        workflow: WorkflowBase
        if message.workflow_name == 'GwasFederated':
            workflow = GwasFederated(self.api, self.adapter, self.schema)
        else:
            raise ValueError("Unknown workflow {}".format(message.workflow_name))

        workflow.execute(message)