from src.Message.BaseMessage import BaseMessage
from src.auto.auto_api_client.model.runner_message import RunnerMessage


class MaterializeCte(BaseMessage):
    table_name: str
    materialized_as: str
    cte: str
    dependency_ctes: dict = None
    indexes: list
    job_id: int
    biobank_data_table_id: int

    def __init__(self, message: RunnerMessage):
        self.type = 'MaterializeCte'
        self.table_name = message.data['tableName']
        self.materialized_as = message.data['materializedAs']
        self.cte = message.data['cte']
        if 'dependencyCtes' in message.data:
            self.dependency_ctes = message.data['dependencyCtes']
        self.indexes = message.data.get('indexes', [])
        self.job_id = int(message.data['jobId'])
        self.biobank_data_table_id = int(message.data['biobankDataTableId'])
        self.runner_message_id = message.id
