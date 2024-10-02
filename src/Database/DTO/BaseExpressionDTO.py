class BaseExpressionDTO:
    type: str
    value: str

    def __init__(self, type: str, value: str):
        self.type = type
        self.value = value