from typing import List

from src.Database.DTO.CreateTableColumnDTO import CreateTableColumnDTO

class CreateTableDTO:
    table_name: str
    columns: List[CreateTableColumnDTO]

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.columns = []