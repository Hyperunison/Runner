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
        lst = self.fetch_all(sql)
        result: List[str] = []
        for i in lst:
            result.append(i['TABLE_NAME'])

        return result

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        sql = "select count(*) as cnt from {}".format(table_name)
        count = self.fetch_row(sql)['cnt']

        database_name = self.dsn_parser.get_database_name(self.dsn)
        sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{}'
            AND table_name = '{}';
        """.format(database_name, table_name)
        lst = self.fetch_all(sql)
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

        count = self.fetch_row(sql_count)['cnt']
        columns: List[Dict[str, str]] = []
        row = self.fetch_row(sql_columns)

        if row:
            for col in row:
                sql_column_info = "WITH {} AS ({}) SELECT '{}' AS type_info, {} FROM {}".format(
                    table_name, cte, col, col, table_name
                )
                type_row = self.fetch_row(sql_column_info)

                item: Dict[str, str] = {}
                item['column'] = col
                item['type'] = type_row['type_info']
                item['nullable'] = True
                columns.append(item)

        return count, columns

    def get_table_column_stats(self, table_name: str, column_name: str, cte: str) -> TableStat:
        sql = "SELECT count(distinct `{v}`) as unique_count from {table}".format(v=column_name, table=table_name)
        sql = self.wrap_sql_by_cte(sql, table_name, cte)
        with_cte_label = 'with CTE' if cte else ''
        try:
            row = self.fetch_row(sql)
        except ProgrammingError as e:
            self.engine.rollback()
            if isinstance(e.orig, UndefinedTable):
                stat = TableStat()
                stat.table_name = table_name
                stat.column_name = column_name
                stat.abandoned = True
                return stat

            else:
                raise e

        unique_count = row['unique_count']

        try:
            sql = "SELECT min(`{v}`) as min_value, max(`{v}`) as max_value, avg(`{v}`) as avg_value from {table}".format(v=column_name, table=table_name)
            sql = self.wrap_sql_by_cte(sql, table_name, cte)
            row = self.fetch_row(sql)

            min_value = row['min_value']
            max_value = row['max_value']
            avg_value = row['avg_value']

            if not self.is_numeric(min_value) or not self.is_numeric(max_value) or not self.is_numeric(avg_value):
                print(min_value, type(min_value))
                print(max_value, type(max_value))
                print(avg_value, type(avg_value))
                min_value = None
                max_value = None
                avg_value = None
                median50_value = None
                median25_value = None
                median12_value = None
                median37_value = None
                median75_value = None
                median63_value = None
                median88_value = None
            else:
                median50_value = self.get_median(table_name, column_name, min_value, max_value, cte)
                median25_value = self.get_median(table_name, column_name, min_value, median50_value, cte)
                median12_value = self.get_median(table_name, column_name, min_value, median25_value, cte)
                median37_value = self.get_median(table_name, column_name, median25_value, median50_value, cte)
                median75_value = self.get_median(table_name, column_name, median50_value, max_value, cte)
                median63_value = self.get_median(table_name, column_name, median50_value, median75_value, cte)
                median88_value = self.get_median(table_name, column_name, median75_value, max_value, cte)
                values_counts = []
        except ProgrammingError as e:
            logging.debug("Can't get min/max values for {}.{} {}".format(table_name, column_name, with_cte_label))
            self.engine.rollback()
            if not isinstance(e.orig, UndefinedFunction):
                raise e
            min_value=None
            max_value=None
            avg_value=None
            median50_value=None
            median25_value=None
            median12_value=None
            median37_value=None
            median75_value=None
            median63_value=None
            median88_value=None

        sql = "SELECT `{}` as value, count(*) as cnt from {} WHERE NOT `{}` IS NULL GROUP BY 1 HAVING COUNT(*) >= {} ORDER BY 1 DESC LIMIT 100".format(
            column_name, table_name, column_name, self.min_count)
        sql = self.wrap_sql_by_cte(sql, table_name, cte)
        logging.debug(sql)
        values_counts = self.fetch_all(sql)
        logging.info("Frequent values counts for {}.{} {}: {}".format(
            table_name,
            column_name,
            with_cte_label,
            (values_counts)
        ))
        nulls_count_sql = "SELECT COUNT(*) as cnt FROM {} WHERE `{}` is null".format(table_name, column_name)
        nulls_count_sql = self.wrap_sql_by_cte(nulls_count_sql, table_name, cte)
        nulls_count = self.fetch_row(nulls_count_sql)['cnt']

        stat = TableStat()
        stat.table_name = table_name
        stat.column_name = column_name
        stat.abandoned = False
        stat.unique_count = unique_count
        stat.nulls_count = nulls_count
        stat.min_value = min_value
        stat.max_value = max_value
        stat.avg_value = avg_value
        stat.median12_value = median12_value
        stat.median25_value = median25_value
        stat.median37_value = median37_value
        stat.median50_value = median50_value
        stat.median63_value = median63_value
        stat.median75_value = median75_value
        stat.median88_value = median88_value
        stat.values = values_counts
        return stat

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

        return self.fetch_row(sql)['median']

    def sql_expression_interval(self, count: str, unit: str) -> str:
        return "'{} {}'::interval".format(count, unit)

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        return "({})::{}".format(expression, data_type)

    def is_numeric(self, value) -> bool:
        return isinstance(value, (int, float, complex, decimal.Decimal))