from src.Message.BaseMessage import BaseMessage
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.KillJob import KillJob
from src.Message.NextflowRun import NextflowRun
from src.Message.StartMLTrain import StartMLTrain
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class MessageFactory:
    def create_message_object_from_response(self, message: RunnerMessage) -> BaseMessage:
        if message.type == 'NextflowRun':
            return NextflowRun(message)
        elif message.type == 'GetProcessLogs':
            return GetProcessLogs(message)
        elif message.type == 'KillJob':
            return KillJob(message)
        elif message.type == 'CohortAPIRequest':
            return CohortAPIRequest(message)
        elif message.type == 'StartMlTrain':
            return StartMLTrain(message)
        else:
            return
