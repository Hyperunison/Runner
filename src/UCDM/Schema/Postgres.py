import logging
from typing import List, Dict, Tuple
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from src.UCDM.Schema.Database import Database
from psycopg2.errors import UndefinedFunction, UndefinedTable
from src.UCDM.TableStat import TableStat

Base = declarative_base()

class Postgres(Database):
    type = 'postgres'
    dsn = ''

    def __init__(self, dsn: str, min_count: int):
        self.dsn = dsn
        self.engine = create_engine(dsn, isolation_level="AUTOCOMMIT").connect()
        super().__init__(dsn, min_count)

    def get_tables_list(self) -> List[str]:
        sql = "SELECT table_schema || '.' || table_name as tbl FROM information_schema.tables WHERE table_type = 'BASE TABLE'" + \
              "AND table_schema NOT IN ('pg_catalog', 'information_schema');"
        lst = self.fetch_all(sql)
        result: List[str] = []
        for i in lst:
            result.append(i['tbl'])

        return result

    def get_table_columns(self, table_name: str) -> Tuple[int, List[Dict[str, str]]]:
        sql = "select count(*) as cnt from {}".format(table_name)
        count = self.fetch_row(sql)['cnt']

        if '.' in table_name:
            schema, table = table_name.split('.')
            sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{}' and table_schema='{}'".format(table, schema)
        else:
            sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{}'".format(table_name)

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
                sql_column_info = "WITH {} AS ({}) SELECT pg_typeof({}) AS pg_typeof, {} FROM {}".format(
                    table_name, cte, col, col, table_name
                )
                type_row = self.fetch_row(sql_column_info)

                item: Dict[str, str] = {}
                item['column'] = col
                item['type'] = type_row['pg_typeof']
                item['nullable'] = True
                columns.append(item)

        return count, columns

    def sql_expression_interval(self, count: str, unit: str) -> str:
        return "'{} {}'::interval".format(count, unit)

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        return "({})::{}".format(expression, data_type)

    def get_median_query(self, table: str, column: str, min, max, cte: str) -> str:
        return "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY {column}) as median FROM {table} WHERE {column} > {min} and {column} < {max}".format(column='"'+column+'"', table=table, min=min, max=max)

    def get_unique_values_count_query(self, table_name: str, column_name: str) -> str:
        return "SELECT count(distinct \"{v}\") as unique_count from {table}".format(v=column_name, table=table_name)

    def get_min_max_avg_value_query(self, table_name: str, column_name: str, cte: str) -> str:
        sql = "SELECT min(\"{v}\") as min_value, max(\"{v}\") as max_value, avg(\"{v}\") as avg_value from {table}".format(
            v=column_name, table=table_name
        )
        return self.wrap_sql_by_cte(sql, table_name, cte)

    def get_values_count_query(self, table_name: str, column_name: str, cte: str) -> str:
        return "SELECT \"{}\" as value, count(*) as cnt from {} WHERE NOT \"{}\" IS NULL GROUP BY 1 HAVING COUNT(*) >= {} ORDER BY 1 DESC LIMIT 100".format(
            column_name, table_name, column_name, self.min_count)

    def get_nulls_count_query(self, table_name: str, column_name: str, cte: str) -> str:
        nulls_count_sql = "SELECT COUNT(*) as cnt FROM {} WHERE \"{}\" is null".format(table_name, column_name)
        return self.wrap_sql_by_cte(nulls_count_sql, table_name, cte)