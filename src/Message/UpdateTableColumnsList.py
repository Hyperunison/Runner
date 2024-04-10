from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class UpdateTableColumnsList(BaseMessage):
    table_name: str

    def __init__(self, message: RunnerMessage):
        self.type = 'UpdateTablesList'
        self.table_name = message.data['tableName']


