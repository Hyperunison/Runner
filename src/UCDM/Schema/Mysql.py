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
            result.append(i['table_name'])

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
            item['column'] = i['column_name']
            item['type'] = i['data_type']
            item['nullable'] = i['is_nullable'] == 'YES'
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

    def sql_expression_interval(self, count: str, unit: str) -> str:
        return "'{} {}'::interval".format(count, unit)

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        return "({})::{}".format(expression, data_type)
