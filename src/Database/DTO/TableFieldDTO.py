class TableFieldDTO:
    table_name: str
    field_name: str

    def __init__(self, table_name: str, field_name: str):
        self.table_name = table_name
        self.field_name = field_name