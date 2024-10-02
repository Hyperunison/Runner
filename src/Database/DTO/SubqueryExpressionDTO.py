from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO

class SubqueryExpressionDTO(BaseExpressionDTO):
    def __init__(self, value: str):
        super().__init__(type='subquery', value=value)