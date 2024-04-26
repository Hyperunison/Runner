from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class KillCohortAPIRequest(BaseMessage):
    pid: int
    cohort_api_request_id: int

    def __init__(self, message: RunnerMessage):
        self.type = 'KillCohortAPIRequest'
        self.cohort_api_request_id = message.data['cohortApiRequestId']
        self.pid = int(message.data['pid'])