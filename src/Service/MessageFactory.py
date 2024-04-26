from src.Message.BaseMessage import BaseMessage
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.Message.KillCohortAPIRequest import KillCohortAPIRequest
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.KillJob import KillJob
from src.Message.NextflowRun import NextflowRun
from src.Message.StartMLTrain import StartMLTrain
from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Message.StartWorkflow import StartWorkflow
from src.Message.UpdateTableColumnStats import UpdateTableColumnStats
from src.Message.UpdateTableColumnsList import UpdateTableColumnsList
from src.Message.UpdateTablesList import UpdateTablesList
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
        elif message.type == 'KillCohortAPIRequest':
            return KillCohortAPIRequest(message)
        elif message.type == 'StartMlTrain':
            return StartMLTrain(message)
        elif message.type == 'StartNextflowCohortWorkflow':
            return StartWorkflow(message)
        elif message.type == 'StartOMOPoficationWorkflow':
            return StartOMOPoficationWorkflow(message)
        elif message.type == 'UpdateTablesList':
            return UpdateTablesList(message)
        elif message.type == 'UpdateTableColumnsList':
            return UpdateTableColumnsList(message)
        elif message.type == 'UpdateTableColumnStats':
            return UpdateTableColumnStats(message)
        else:
            return
