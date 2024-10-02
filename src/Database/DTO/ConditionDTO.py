from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO

class ConditionDTO:
    left: BaseExpressionDTO
    operation: str
    right: BaseExpressionDTO

    def __init__(self, left: BaseExpressionDTO, operation: str, right: BaseExpressionDTO):
        self.left = left
        self.operation = operation
        self.right = right

        if self.if_operation_valid(operation):
            raise "Invalid operation"

    def if_operation_valid(self, operation: str) -> bool:
        valid_operations = ["==", "!=", "<", "<=", ">", ">="]

        return operation in valid_operations