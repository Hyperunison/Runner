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
    run_dqd: str
    server_data_links: Dict[str, str]
    cdm_id: int

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
        self.run_dqd = message.data['runDQD']
        self.server_data_links = message.data['serverDataLinks'] if 'serverDataLinks' in message.data else []
        self.cdm_id = int(message.data['cdmId']) if 'cdmId' in message.data else None

    def does_server_data_omop_concept_exist(self) -> bool:
        if self.server_data_links is None:
            return False

        return 'omop-concept' in self.server_data_links

    def does_server_data_omop_vocabularies_exist(self) -> bool:
        if self.server_data_links is None:
            return False

        return 'omop-vocabularies' in self.server_data_links


