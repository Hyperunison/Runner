import re
from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO
from src.Database.DTO.ConstantExpressionDTO import ConstantExpressionDTO
from src.Database.DTO.FieldExpressionDTO import FieldExpressionDTO
from src.Database.DTO.SubqueryExpressionDTO import SubqueryExpressionDTO

class SqlExpressionParser:

    def parse(self, expr: str) -> BaseExpressionDTO:
        expr = expr.strip()

        if expr.startswith('(') and 'SELECT' in expr.upper():
            return SubqueryExpressionDTO(expr)

        # Check if it's a string literal (enclosed in single quotes)
        if expr.startswith("'") and expr.endswith("'"):
            return ConstantExpressionDTO(expr, 'string')

        # Check if it's a number (integer or float)
        if re.match(r'^\d+(\.\d+)?$', expr):
            return ConstantExpressionDTO(expr, 'number')

        # Check if it's a boolean (TRUE or FALSE)
        if expr.upper() in ['TRUE', 'FALSE']:
            return ConstantExpressionDTO(expr, 'boolean')

        if re.match(r'^[\w]+(\.[\w]+){0,2}$', expr):
            return FieldExpressionDTO(expr)

        raise ValueError(f'Invalid expression: {expr}')