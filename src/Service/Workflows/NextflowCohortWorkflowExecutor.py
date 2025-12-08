from src.Api import Api
from src.Message.StartWorkflow import StartWorkflow
from src.Service.Workflows.CreateSQLViews.CreateSQLViews import CreateSQLViews
from src.Service.Workflows.PipelineExecutor import PipelineExecutor
from src.Service.Workflows.VendorPipelines import VendorPipelines
from src.Service.Workflows.GwasFederated.GwasFederated import GwasFederated
from src.Service.Workflows.OMOPification.OMOPofication import OMOPofication
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.UCDM.DataSchema import DataSchema


class NextflowCohortWorkflowExecutor:

    api: Api
    pipeline_executor: PipelineExecutor
    schema: DataSchema
    vendor_pipelines: VendorPipelines

    def __init__(self, api: Api, pipeline_executor: PipelineExecutor, schema: DataSchema, vendor_pipelines: VendorPipelines):
        self.api = api
        self.pipeline_executor = pipeline_executor
        self.schema = schema
        self.vendor_pipelines = vendor_pipelines

    def execute_workflow(self, message: StartWorkflow, may_upload_private_data: bool):
        workflow: WorkflowBase
        if message.workflow_name == 'GwasFederated':
            workflow = GwasFederated(self.api, self.pipeline_executor, self.schema)
            workflow.execute(message, self.api)
            return

        elif message.workflow_name == 'OMOPification':
            workflow = OMOPofication(self.api, self.pipeline_executor, self.schema, may_upload_private_data)
            workflow.execute(message, self.api)
            return

        elif message.workflow_name == 'CreateSQLViews':
            workflow = CreateSQLViews(self.api, self.pipeline_executor, self.schema)
            workflow.execute(message, self.api)
            return

        workflow = self.vendor_pipelines.find_pipeline(message.workflow_name, message.workflow_version)
        if workflow is None:
            raise ValueError("Unknown workflow {}".format(message.workflow_name))

        workflow.execute(message, self.api)
