from typing import List, Dict, Tuple
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.exc import OperationalError

from src.Database.Utils.DsnParser import DsnParser
from src.UCDM.Exception.NonNumericField import NonNumericField
from src.UCDM.Schema.Database import Database

Base = declarative_base()

class Hive(Database):
    type = 'hive'
    dsn = ''
    dsn_parser: DsnParser

    def __init__(self, dsn: str, min_count: int):
        self.dsn = dsn
        self.engine = create_engine(dsn).connect()
        self.dsn_parser = DsnParser()
        super().__init__(dsn, min_count)

    def get_tables_list(self) -> List[str]:
        sql = "show tables"
        lst = self.fetch_all(sql)
        result: List[str] = []
        for i in lst:
            result.append(i['tableName'])

        return result

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        sql_columns = "DESCRIBE {}".format(table_name)
        sql_count = "SELECT COUNT(*) AS cnt from {}".format(table_name)

        count = self.fetch_row(sql_count)['cnt']
        columns: List[Dict[str, str]] = []
        list = self.fetch_all(sql_columns)

        for column in list:
            col: str = column['col_name']
            sql_isnull = "SELECT count(*) as cnt FROM {} WHERE {} IS NULL LIMIT 1".format(table_name, col)
            isnull_row = self.fetch_row(sql_isnull)

            item: Dict[str, str] = {}
            item['column'] = col
            item['type'] = column['data_type']
            item['nullable'] = isnull_row['cnt'] > 0
            columns.append(item)

        return count, columns

    def get_cte_columns(self, table_name: str, cte: str) -> Tuple[int, List[Dict[str, str]]]:
        sql_columns = "WITH {} AS ({}) SELECT * from {} LIMIT 1".format(table_name, cte, table_name)
        sql_count = "WITH {} AS ({}) SELECT COUNT(*) AS cnt from {}".format(table_name, cte, table_name)

        count = self.fetch_row(sql_count)['cnt']
        columns: List[Dict[str, str]] = []
        row = self.fetch_row(sql_columns)

        if row:
            for col in row:
                sql_column_info = "WITH {} AS ({}) SELECT '{}' AS type_info, {} FROM {}".format(
                    table_name, cte, col, col, table_name
                )
                type_row = self.fetch_row(sql_column_info)

                sql_isnull = "SELECT count(*) as cnt FROM {} WHERE {} IS NULL LIMIT 1".format(table_name, col)
                isnull_row = self.fetch_row(sql_isnull)

                item: Dict[str, str] = {}
                item['column'] = col
                item['type'] = type_row['type_info']
                item['nullable'] = isnull_row['cnt'] > 0
                columns.append(item)

        return count, columns

    def get_median(self, table: str, column: str, min, max, cte: str):
        try:
            return super().get_median(table, column, min, max, cte)
        except OperationalError as e:
            if 'DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE' in str(e):
                raise NonNumericField(str(e), orig=e, params=e.params)
            else:
                raise e

    def get_min_max_avg_value(self, table_name: str, column_name: str, cte: str) -> Dict[str, any]:
        try:
            return super().get_min_max_avg_value(table_name, column_name, cte)
        except OperationalError as e:
            if 'DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE' in str(e):
                raise NonNumericField(str(e), orig=e, params=e.params)
            else:
                raise e

    def sql_expression_interval(self, count: str, unit: str) -> str:
        return "INTERVAL '{} {}'".format(count, unit)

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        return "CAST({} AS {})".format(expression, data_type)

    def get_median_query(self, table: str, column: str, min, max, cte: str) -> str:
        return "SELECT approx_percentile(`{column}`, 0.5) as median FROM `{table}` WHERE `{column}` > {min} and {column} < {max}".format(
            column=column, table=table, min=min, max=max)

    def get_unique_values_count_query(self, table_name: str, column_name: str) -> str:
        return "SELECT count(distinct `{v}`) as unique_count from {table}".format(v=column_name, table=table_name)

    def get_min_max_avg_value_query(self, table_name: str, column_name: str, cte: str) -> str:
        sql = "SELECT min(`{v}`) as min_value, max(`{v}`) as max_value, avg(`{v}`) as avg_value from {table}".format(
            v=column_name, table=table_name
        )
        return self.wrap_sql_by_cte(sql, table_name, cte)

    def get_values_count_query(self, table_name: str, column_name: str, cte: str) -> str:
        return "SELECT `{}` as value, count(*) as cnt from {} WHERE NOT `{}` IS NULL GROUP BY 1 HAVING COUNT(*) >= {} ORDER BY 1 DESC LIMIT 100".format(
            column_name, table_name, column_name, self.min_count)

    def get_nulls_count_query(self, table_name: str, column_name: str, cte: str) -> str:
        nulls_count_sql = "SELECT COUNT(*) as cnt FROM {} WHERE `{}` is null".format(table_name, column_name)
        return self.wrap_sql_by_cte(nulls_count_sql, table_name, cte)