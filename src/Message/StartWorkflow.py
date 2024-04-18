from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from typing import List, Dict


class StartWorkflow(BaseMessage):
    run_id: int
    dir: str
    s3_path: str
    workflow_name: str
    workflow_version: str
    cohort_definition: any
    parameters: Dict[str, str]
    run_name: str
    weblog_url: str

    def __init__(self, message: RunnerMessage):
        self.type = 'StartWorkflow'
        self.run_id = int(message.data['runId'])
        self.dir = message.data['dir']
        self.s3_path = message.data['s3Path']
        self.workflow_name = message.data['workflowName']
        self.workflow_version = message.data['workflowVersion']
        self.cohort_definition = message.data['cohortDefinition']
        self.parameters = message.data['parameters']
        self.run_name = message.data['runName']
        self.weblog_url = message.data['weblogUrl']
        self.aws_id = message.data['awsId']
        self.aws_key = message.data['awsKey']


