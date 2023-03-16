from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class GetProcessLogs(BaseMessage):
    process_id: str
    reply_channel: str
    lines_limit: int

    def __init__(self, message: RunnerMessage):
        self.type = 'GetProcessLogs'
        self.process_id = message.data['processId']
        self.reply_channel = message.data['replyChannel']
        self.lines_limit = message.data['linesLimit']

