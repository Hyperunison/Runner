class CreateTableColumnDTO:
    name: str
    type: str
    nullable: bool

    def __init__(self, name: str, type: str, nullable: bool):
        self.name = name
        self.type = type
        self.nullable = nullable