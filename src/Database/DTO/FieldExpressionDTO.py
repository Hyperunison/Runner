from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO

class FieldExpressionDTO(BaseExpressionDTO):
    def __init__(self, value: str):
        super().__init__(type='field', value=value)