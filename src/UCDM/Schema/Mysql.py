import decimal
import logging
from decimal import Decimal
from typing import List, Dict, Tuple
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.errors import UndefinedFunction, UndefinedTable

from src.Database.Utils.DsnParser import DsnParser
from src.UCDM.Exception.NonNumericField import NonNumericField
from src.UCDM.Schema.BaseSchema import BaseSchema
from src.UCDM.Schema.Database import Database
from src.UCDM.TableStat import TableStat
import datetime

Base = declarative_base()

class Mysql(Database):
    type = 'mysql'
    dsn = ''
    dsn_parser: DsnParser

    def __init__(self, dsn: str, min_count: int):
        self.dsn = dsn
        self.engine = create_engine(dsn, isolation_level="AUTOCOMMIT").connect()
        self.dsn_parser = DsnParser()
        super().__init__(dsn, min_count)

    def get_tables_list(self) -> List[str]:
        database_name = self.dsn_parser.get_database_name(self.dsn)
        sql = """SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{}';""".format(database_name)
        lst = self.fetch_all_deprecated(sql)
        result: List[str] = []
        for i in lst:
            result.append(i['TABLE_NAME'])

        return result

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        sql = "select count(*) as cnt from {}".format(table_name)
        count = self.fetch_row_deprecated(sql)['cnt']

        database_name = self.dsn_parser.get_database_name(self.dsn)
        sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{}'
            AND table_name = '{}';
        """.format(database_name, table_name)
        lst = self.fetch_all_deprecated(sql)
        columns: List[Dict[str, str]] = []
        for i in lst:
            item: Dict[str, str] = {}
            item['column'] = i['COLUMN_NAME']
            item['type'] = i['DATA_TYPE']
            item['nullable'] = i['IS_NULLABLE'] == 'YES'
            columns.append(item)

        return count, columns

    def get_cte_columns(self, table_name: str, cte: str) -> Tuple[int, List[Dict[str, str]]]:
        sql_columns = "WITH {} AS ({}) SELECT * from {} LIMIT 1".format(table_name, cte, table_name)
        sql_count = "WITH {} AS ({}) SELECT COUNT(*) AS cnt from {}".format(table_name, cte, table_name)

        count = self.fetch_row_deprecated(sql_count)['cnt']
        columns: List[Dict[str, str]] = []
        row = self.fetch_row_deprecated(sql_columns)

        if row:
            for col in row:
                sql_column_info = "WITH {} AS ({}) SELECT '{}' AS type_info, {} FROM {} LIMIT 1".format(
                    table_name, cte, col, col, table_name
                )
                type_row = self.fetch_row_deprecated(sql_column_info)

                item: Dict[str, str] = {}
                item['column'] = col
                item['type'] = type_row['type_info']
                item['nullable'] = True
                columns.append(item)

        return count, columns

    def get_median(self, table: str, column: str, min, max, cte: str):
        if min is None or max is None:
            return None
        sql = """
            SELECT AVG(middle_values.{column}) AS median
            FROM (
                SELECT {column}, 
                       ROW_NUMBER() OVER (ORDER BY {column}) AS row_num,
                       (SELECT COUNT(*) FROM `{table}` WHERE {column} > {min} AND {column} < {max}) AS total_rows
                FROM `{table}`
                WHERE {column} > {min} AND {column} < {max}
            ) AS middle_values
            WHERE middle_values.row_num IN (
                FLOOR((total_rows + 1) / 2), 
                FLOOR((total_rows + 2) / 2)
            )
        """
        sql = sql.replace("{column}", '`' + column + '`')
        sql = sql.replace("{table}", table)
        sql = sql.replace("{max}", str(max))
        sql = sql.replace("{min}", str(min))

        sql = self.wrap_sql_by_cte(sql, table, cte)

        return self.fetch_row_deprecated(sql)['median']

    def get_min_max_avg_value(self, table_name: str, column_name: str, cte: str) -> Dict[str, any]:
        try:
            return super().get_min_max_avg_value(table_name, column_name, cte)
        except ProgrammingError as e:
            if isinstance(e.orig, UndefinedFunction):
                raise NonNumericField(str(e), params=e.params, orig=e)
            raise e

    def sql_expression_interval(self, count: str, unit: str) -> str:
        return "INTERVAL '{} {}'".format(count, unit)

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        return "({})::{}".format(expression, data_type)

    def get_median_query(self, table: str, column: str, min, max, cte: str) -> str:
        sql = """
                    SELECT AVG(middle_values.{column}) AS median
                    FROM (
                        SELECT {column}, 
                               ROW_NUMBER() OVER (ORDER BY {column}) AS row_num,
                               (SELECT COUNT(*) FROM `{table}` WHERE {column} > {min} AND {column} < {max}) AS total_rows
                        FROM `{table}`
                        WHERE {column} > {min} AND {column} < {max}
                    ) AS middle_values
                    WHERE middle_values.row_num IN (
                        FLOOR((total_rows + 1) / 2), 
                        FLOOR((total_rows + 2) / 2)
                    )
                """
        sql = sql.replace("{column}", '`' + column + '`')
        sql = sql.replace("{table}", table)
        sql = sql.replace("{max}", str(max))

        return sql.replace("{min}", str(min))

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