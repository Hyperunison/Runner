from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class NextflowRun(BaseMessage):
    run_id: int
    command: str
    dataset_id: int
    dir: str
    input_data: object
    nextflow_code: str
    aws_id: str
    aws_key: str

    def __init__(self, message: RunnerMessage):
        self.type = 'NextflowRun'
        self.command = message.data['command']
        self.dataset_id = message.data['datasetId']
        self.dir = message.data['dir']
        self.input_data = message.data['inputData']
        self.nextflow_code = message.data['nextflowCode']
        self.run_id = int(message.data['runId'])
        self.aws_id = message.data['awsId']
        self.aws_key = message.data['awsKey']