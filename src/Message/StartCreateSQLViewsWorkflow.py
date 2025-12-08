from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from typing import List, Dict


class StartCreateSQLViewsWorkflow(BaseMessage):
    queries: Dict[str, any]
    reply_channel: str
    id: int
    connection_string: str
    all_tables: List[Dict[str, any]]
    automation_strategies_map: Dict[int, Dict[str, Dict[str, str]]]
    cdm_id: int

    def __init__(self, message: RunnerMessage):
        self.type = 'StartCreateSQLViewsWorkflow'
        self.id = message.id
        self.workflow_name = message.data['workflowName']
        self.queries = message.data['queries']
        self.reply_channel = message.data['replyChannel']
        self.connection_string = message.data['connectionString']
        self.all_tables = message.data['allTables']
        self.automation_strategies_map = message.data['automationStrategiesMap']
        self.cdm_id = int(message.data['cdmId']) if 'cdmId' in message.data else None
