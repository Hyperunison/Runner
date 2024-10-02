from typing import List

from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO
from src.Database.DTO.ConditionDTO import ConditionDTO

class JoinDTO:
    type: str
    table_name: str
    table: BaseExpressionDTO
    alias: str
    conditions: List[ConditionDTO]

    def __init__(self, table_name: str, alias: str):
        self.type = 'inner'
        self.table_name = table_name
        self.alias = alias
        self.conditions = []