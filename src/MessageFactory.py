from src.Message.BaseMessage import BaseMessage
from src.Message.CreateFolder import CreateFolder
from src.Message.NextflowRun import NextflowRun
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class MessageFactory:
    def create_message_object_from_response(self, message: RunnerMessage) -> BaseMessage:
        if message.type == 'NextflowRun':
            return NextflowRun(message)
        elif message.type == 'CreateFolder':
            return CreateFolder(message)
        else:
            return