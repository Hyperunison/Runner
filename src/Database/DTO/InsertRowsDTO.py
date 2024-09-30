from typing import List, Dict

class InsertRowsDTO:
    table_name: str
    rows: List[Dict[str, any]]

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.rows = []