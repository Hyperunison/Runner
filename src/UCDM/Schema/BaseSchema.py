from typing import List, Dict, Tuple
from src.UCDM import TableStat


class BaseSchema:
    type = ''
    engine = None
    min_count: int = 0

    known_functions = []

    def __init__(self, dsn: str, min_count: int):
        self.min_count = min_count
        super().__init__()

    def fetch_all(self, sql: str):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def get_table_column_stats(self, table_name: str, column_name: str) -> TableStat:
        raise NotImplementedError()

    def get_table_cte_column_stats(self, table_name: str, cte: str, column_name: str) -> TableStat:
        raise NotImplementedError()

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        raise NotImplementedError()

    def get_cte_columns(self, table_name: str, cte: str) -> Tuple[int, List[Dict[str, str]]]:
        raise NotImplementedError()

    def get_tables_list(self) -> List[str]:
        raise NotImplementedError()

    def get_median(self, table: str, column: str, min, max):
        raise NotImplementedError()

    def fetch_row(self, sql: str) -> Dict:
        raise NotImplementedError()

    def reconnect(self):
        raise NotImplementedError()

    def sql_expression_interval(self, count: str, unit: str) -> str:
        raise NotImplementedError()

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        raise NotImplementedError()

    def statement_callback(self, statement) -> Dict:
        return statement