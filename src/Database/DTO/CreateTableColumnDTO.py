class CreateTableColumnDTO:
    name: str
    type: str
    nullable: bool
    primary_key: bool

    def __init__(self, name: str, type: str, nullable: bool, primary_key: bool = False):
        self.name = name
        self.type = type
        self.nullable = nullable
        self.primary_key = primary_key