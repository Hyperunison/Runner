from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO

class ConstantExpressionDTO(BaseExpressionDTO):
    variable_type: str

    def __init__(self, value: str, variable_type: str):
        super().__init__(type='constant', value=value)
        self.variable_type = variable_type