from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class UpdateTableColumnsList(BaseMessage):
    table_name: str
    cte: str = None

    def __init__(self, message: RunnerMessage):
        self.type = 'UpdateTablesList'
        self.table_name = message.data['tableName']
        if 'cte' in message.data:
            self.cte = message.data['cte']
