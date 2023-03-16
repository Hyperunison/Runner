from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class KillJob(BaseMessage):
    run_id: int
    channel: str

    def __init__(self, message: RunnerMessage):
        self.type = 'KillJob'
        self.run_id = message.data['runId']
        self.channel = message.data['channel']


