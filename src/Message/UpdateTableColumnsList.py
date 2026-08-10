from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class UpdateTableColumnsList(BaseMessage):
    table_name: str
    cte: str = None
    dependency_ctes: dict = None
    materialized_as: str = None

    def __init__(self, message: RunnerMessage):
        self.type = 'UpdateTablesList'
        self.table_name = message.data['tableName']
        if 'cte' in message.data:
            self.cte = message.data['cte']
        if 'dependencyCtes' in message.data:
            self.dependency_ctes = message.data['dependencyCtes']
        if 'materializedAs' in message.data:
            self.materialized_as = message.data['materializedAs']
