from typing import List, Dict, Tuple, Optional

from src.Helpers import SQLWithParameters
from src.UCDM import TableStat
from psycopg2.extensions import adapt


class BaseSchema:
    type = ''
    engine = None
    min_count: int = 0
    dsn: str

    known_functions = []

    def __init__(self, dsn: str, min_count: int):
        self.min_count = min_count
        self.dsn = dsn
        super().__init__()

    def fetch_all_deprecated(self, sql: str):
        raise NotImplementedError()

    def fetch_all(self, query: SQLWithParameters):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def get_table_column_stats(self, table_name: str, column_name: str, cte: str) -> TableStat:
        raise NotImplementedError()

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        raise NotImplementedError()

    def get_cte_columns(self, table_name: str, cte: str) -> Tuple[int, List[Dict[str, str]]]:
        raise NotImplementedError()

    def get_tables_list(self) -> List[str]:
        raise NotImplementedError()

    def get_median(self, table: str, column: str, min, max, cte: str):
        raise NotImplementedError()

    def fetch_row_deprecated(self, sql: str) -> Dict:
        raise NotImplementedError()

    def fetch_row(self, query: SQLWithParameters) -> Dict:
        raise NotImplementedError()

    def reconnect(self):
        raise NotImplementedError()

    def sql_expression_interval(self, count: str, unit: str) -> SQLWithParameters:
        raise NotImplementedError()

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> SQLWithParameters:
        raise NotImplementedError()

    def statement_callback(self, statement) -> Dict:
        return statement

    def wrap_sql_by_cte(self, sql: str, table_name: str, cte: str) -> str:
        if cte:
            return "WITH {table_name} AS ({cte}) {origin_sql}".format(
                table_name=table_name,
                cte=cte,
                origin_sql=sql
            )

        return sql

    def execute_sql_deprecated(self, sql: str) -> str:
        raise NotImplementedError()

    def execute_sql(self, query: SQLWithParameters) -> str:
        raise NotImplementedError()

    def escape_table_name(self, table_name: str) -> str:
        raise Exception("Not implemented")

    def escape_column_name(self, table_name: str) -> str:
        raise Exception("Not implemented")

    # for logging and GUI only
    def build_parameters_inside_sql(self, sql: str, parameters: Dict[str, any]) -> str:
        def escape(val):
            if val is None:
                return 'NULL'
            return adapt(val).getquoted().decode()

        final_query = sql
        for key, value in parameters.items():
            placeholder = f":{key}"
            quoted_value = escape(value)
            # Replace all occurrences of :key with the quoted value
            final_query = final_query.replace(placeholder, quoted_value)
        return final_query