from typing import List, Optional, Dict

from src.Database.DTO.ConditionDTO import ConditionDTO
from src.Database.DTO.GroupByDTO import GroupByDTO
from src.Database.DTO.JoinDTO import JoinDTO
from src.Database.DTO.OrderByDTO import OrderByDTO
from src.Database.DTO.TableAliasDTO import TableAliasDTO


class SelectDTO:
    table_name: str
    fields: List[str]
    joins: List[JoinDTO]
    conditions: List[ConditionDTO]
    order_by: List[OrderByDTO]
    group_by: GroupByDTO
    limit: Optional[int]
    table_aliases: List[TableAliasDTO]

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.fields = []
        self.joins = []
        self.conditions = []
        self.order_by = []
        self.group_by = GroupByDTO([])
        self.table_aliases = []

    def get_origin_table_by_alias(self, alias: str) -> Optional[TableAliasDTO]:
        for table_alias in self.table_aliases:
            if alias == table_alias.alias:
                return table_alias

        return None