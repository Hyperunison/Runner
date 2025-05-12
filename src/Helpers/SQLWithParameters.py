from typing import Dict, Optional

class SQLWithParameters:
    sql: str
    parameters: Optional[Dict[str, any]] = None

    def __init__(self, sql: str, parameters: Optional[Dict[str, any]] = None):
        self.sql = sql
        self.parameters = parameters