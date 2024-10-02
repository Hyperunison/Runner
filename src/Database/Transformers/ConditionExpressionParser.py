from typing import Optional
import re

from src.Database.DTO.ConditionDTO import ConditionDTO

class ConditionExpressionParser:

    def parse(self, expression: str) -> Optional[ConditionDTO]:
        pattern = r"(\w+\.\w+\.\w+)\s*(=|<>|!=|<|>|<=|>=)\s*(\w+\.\w+)"
        match = re.match(pattern, expression)

        if match:
            left_side = match.group(1)
            operator = match.group(2)
            right_side = match.group(3)
            return ConditionDTO(field_name=left_side, operation=operator, value=right_side)

        return None