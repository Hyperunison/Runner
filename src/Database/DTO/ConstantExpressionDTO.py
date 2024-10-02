from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO

class ConstantExpressionDTO(BaseExpressionDTO):
    def __init__(self, value: str):
        super().__init__(type='constant', value=value)