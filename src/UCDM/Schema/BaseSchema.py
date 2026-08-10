from typing import List, Dict, Tuple, Optional
from src.UCDM import TableStat


class BaseSchema:
    type = ''
    engine = None
    min_count: int = 0
    dsn: str
    _ctes: dict

    known_functions = []

    def __init__(self, dsn: str, min_count: int):
        self.min_count = min_count
        self.dsn = dsn
        self._ctes = {}
        super().__init__()

    def fetch_all(self, sql: str):
        raise NotImplementedError()

    def fetch_chunks(self, sql: str, chunk_size: int):
        # Default fallback: load all rows and yield as a single chunk.
        # Subclasses that support server-side cursors should override this.
        rows = self.fetch_all(sql)
        if rows:
            yield rows

    def rollback(self):
        raise NotImplementedError()

    def get_table_column_stats(self, table_name: str, column_name: str, cte: str) -> TableStat:
        raise NotImplementedError()

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        raise NotImplementedError()

    def get_cte_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        raise NotImplementedError()

    def get_tables_list(self) -> List[str]:
        raise NotImplementedError()

    def get_median(self, table: str, column: str, min, max, cte: str):
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

    def wrap_sql_by_cte(self, sql: str, ctes: dict) -> str:
        """Wrap a SQL statement with a WITH clause built from a {name: cte_sql} dict."""
        if not ctes:
            return sql

        parts = ["{} AS ({})".format(name, cte_sql) for name, cte_sql in ctes.items()]
        return "WITH {} {}".format(", ".join(parts), sql)

    def execute_sql(self, sql: str) -> str:
        raise NotImplementedError()

    def execute_sql_params(self, sql: str, params) -> None:
        raise NotImplementedError()

    def get_materialized_view_definition(self, name: str) -> Optional[str]:
        """Return stored SQL definition of a materialized view, or None if not exists."""
        raise NotImplementedError()

    def create_materialized_view(self, name: str, sql: str) -> None:
        raise NotImplementedError()

    def drop_materialized_view(self, name: str) -> None:
        raise NotImplementedError()

    def refresh_materialized_view(self, name: str) -> None:
        raise NotImplementedError()
