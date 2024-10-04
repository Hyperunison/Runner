from typing import Optional

class TableAliasDTO:
    schema_name: Optional[str]
    table_name: str
    alias: str

    def __init__(self, schema_name: Optional[str], table_name: str, alias: str):
        self.schema_name = schema_name
        self.table_name = table_name
        self.alias = alias