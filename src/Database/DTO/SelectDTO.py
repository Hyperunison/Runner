from typing import List, Optional

from src.Database.DTO.ConditionDTO import ConditionDTO
from src.Database.DTO.GroupByDTO import GroupByDTO
from src.Database.DTO.JoinDTO import JoinDTO
from src.Database.DTO.OrderByDTO import OrderByDTO

class SelectDTO:
    table_name: str
    fields: List[str]
    joins: List[JoinDTO]
    conditions: List[ConditionDTO]
    order_by: List[OrderByDTO]
    group_by: GroupByDTO
    limit: Optional[int]

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.fields = []
        self.joins = []
        self.conditions = []
        self.order_by = []
        self.group_by = GroupByDTO([])