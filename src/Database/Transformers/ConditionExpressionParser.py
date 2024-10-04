from typing import Optional
import re

from src.Database.DTO.ConditionDTO import ConditionDTO
from src.Database.Transformers.SqlExpressionParser import SqlExpressionParser

class ConditionExpressionParser:

    def parse(self, expression: str) -> Optional[ConditionDTO]:
        pattern = r"(\w+\.\w+\.\w+)\s*(=|<>|!=|<|>|<=|>=)\s*(\w+\.\w+)"
        match = re.match(pattern, expression)

        if match:
            left_side = match.group(1)
            operator = match.group(2)
            right_side = match.group(3)
            sql_expression_parser = SqlExpressionParser()
            left_side = sql_expression_parser.parse(left_side)
            right_side = sql_expression_parser.parse(right_side)

            return ConditionDTO(left=left_side, operation=operator, right=right_side)

        return None