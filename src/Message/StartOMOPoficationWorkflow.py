from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from typing import List, Dict


class StartOMOPoficationWorkflow(BaseMessage):
    queries: Dict[str, any]
    reply_channel: str
    id: int
    aws_id: str
    aws_key: str
    s3_bucket: str
    s3_path: str
    format: str
    connection_string: str
    all_tables: List[Dict[str, any]]
    automation_strategies_map: Dict[str, Dict[str, str]]

    def __init__(self, message: RunnerMessage):
        self.type = 'StartOMOPoficationWorkflow'
        self.id = message.id
        self.workflow_name = message.data['workflowName']
        self.queries = message.data['queries']
        self.reply_channel = message.data['replyChannel']
        self.aws_id = message.data['awsId']
        self.aws_key = message.data['awsKey']
        self.s3_bucket = message.data['s3Bucket']
        self.s3_path = message.data['s3Path']
        self.format = message.data['format']
        self.connection_string = message.data['connectionString']
        self.all_tables = message.data['allTables']
        self.automation_strategies_map = message.data['automationStrategiesMap']
        self.automation_strategies_map = message.data['automationStrategiesMap']
        self.run_dqd = message.data['runDQD']


