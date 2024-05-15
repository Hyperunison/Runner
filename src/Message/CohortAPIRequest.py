from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class CohortAPIRequest(BaseMessage):
    cohort_definition: any # todo: change any to class with list predefined static fields
    reply_channel: str
    biobank_id: int
    cohort_api_request_id: int

    def __init__(self, message: RunnerMessage):
        self.type = 'CohortAPIRequest'
        self.cohort_definition = message.data['cohortDefinition']
        self.reply_channel = message.data['replyChannel']
        self.biobank_id = message.data['biobankId']
        self.raw_only = message.data['rawOnly']
        self.cohort_api_request_id = message.data['cohortApiRequestId']
