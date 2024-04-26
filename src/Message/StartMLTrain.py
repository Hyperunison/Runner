from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage
from typing import List, Dict


class StartMLTrain(BaseMessage):
    cohort_definition: any
    ml_train_run_id: int
    model_template: str
    server_address: str
    parameters: Dict[str, str]

    def __init__(self, message: RunnerMessage):
        self.type = 'StartMLTrain'
        self.cohort_definition = message.data['cohortDefinition']
        self.ml_train_run_id = message.data['mlTrainRunId']
        self.model_template = message.data['modelTemplate']
        self.server_address = message.data['serverAddress']
        self.parameters = message.data['parameters']


