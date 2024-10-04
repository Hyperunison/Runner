import re
from typing import Optional
from src.Database.DTO.SubqueryExpressionDTO import SubqueryExpressionDTO

class SubqueryExpressionParser:

    def get_subquery_alias(self, subquery: SubqueryExpressionDTO) -> Optional[str]:
        sql_query = subquery.value.strip()
        pattern = re.compile(r'\)\s+AS\s+(\w+)$', re.IGNORECASE)
        match = pattern.search(sql_query)
        if match:
            return match.group(1)

        return None