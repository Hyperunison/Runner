from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class UpdateTableColumnStats(BaseMessage):
    table_name: str
    column_name: str
    cte: str

    def __init__(self, message: RunnerMessage):
        self.type = 'UpdateTableColumnStats'
        self.table_name = message.data['tableName']
        self.column_name = message.data['columnName']
        self.cte = message.data['cte']


