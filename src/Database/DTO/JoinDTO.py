from typing import List, Optional

from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO
from src.Database.DTO.ConditionDTO import ConditionDTO

class JoinDTO:
    type: str
    table: BaseExpressionDTO
    alias: Optional[str]
    conditions: List[ConditionDTO]

    def __init__(self, table: BaseExpressionDTO, alias: Optional[str]):
        self.type = 'inner'
        self.table = table
        self.alias = alias
        self.conditions = []