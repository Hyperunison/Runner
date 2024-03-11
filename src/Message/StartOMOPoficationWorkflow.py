from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from typing import List, Dict


class StartOMOPoficationWorkflow(BaseMessage):
    queries: Dict[str, any]
    reply_channel: str

    def __init__(self, message: RunnerMessage):
        self.type = 'StartOMOPoficationWorkflow'
        self.workflow_name = message.data['workflowName']
        self.queries = message.data['queries']
        self.reply_channel = message.data['replyChannel']


