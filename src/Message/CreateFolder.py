from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class CreateFolder(BaseMessage):
    folder: str

    def __init__(self, message: RunnerMessage):
        self.type = 'CreateFolder'
        self.folder = message.data['folder']

