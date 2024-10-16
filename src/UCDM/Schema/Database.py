import logging
from decimal import Decimal
from typing import List, Dict, Tuple
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.errors import UndefinedFunction, UndefinedTable

from src.Database.Engines.EngineFacade import EngineFacade
from src.UCDM.Schema.BaseSchema import BaseSchema
from src.UCDM.TableStat import TableStat
import datetime

Base = declarative_base()

class Database(BaseSchema):
    type = 'postgres'
    dsn = ''
    engine_facade: EngineFacade

    def __init__(self, dsn: str, min_count: int):
        self.dsn = dsn
        self.engine = create_engine(dsn, isolation_level="AUTOCOMMIT").connect()
        super().__init__(dsn, min_count)

    def get_table_column_stats(self, table_name: str, column_name: str, cte: str) -> TableStat:
        sql = "SELECT count(distinct \"{v}\") as unique_count from {table}".format(v=column_name, table=table_name)
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
            sql = "SELECT min(\"{v}\") as min_value, max(\"{v}\") as max_value, avg(\"{v}\") as avg_value from {table}".format(v=column_name, table=table_name)
            sql = self.wrap_sql_by_cte(sql, table_name, cte)
            row = self.fetch_row(sql)

            min_value = row['min_value']
            max_value = row['max_value']
            avg_value = row['avg_value']
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

        sql = "SELECT \"{}\" as value, count(*) as cnt from {} WHERE NOT \"{}\" IS NULL GROUP BY 1 HAVING COUNT(*) >= {} ORDER BY 1 DESC LIMIT 100".format(
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
        nulls_count_sql = "SELECT COUNT(*) as cnt FROM {} WHERE \"{}\" is null".format(table_name, column_name)
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
        sql = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY {column}) as median FROM {table} WHERE {column} > {min} and {column} < {max}".format(column='"'+column+'"', table=table, min=min, max=max)
        sql = self.wrap_sql_by_cte(sql, table, cte)

        return self.fetch_row(sql)['median']

    def fetch_row(self, sql: str) -> Dict:
        result = self.engine.execute(text(sql)).mappings().all()
        result = [dict(row) for row in result]

        return result[0]

    def fetch_all(self, sql: str):
        result = self.engine.execute(text(sql)).mappings().all()
        result = [dict(row) for row in result]

        for item in result:
            for key, value in item.items():
                if isinstance(value, datetime.date):
                    item[key] = value.strftime('%Y-%m-%d')
                if isinstance(value, Decimal):
                    if int(value) == float(value):
                        item[key] = int(value)
                    else:
                        item[key] = float(value)

        return result

    def rollback(self):
        self.engine.rollback()

    def reconnect(self):
        self.engine.close()
        self.engine = create_engine(self.dsn).connect()

    def sql_expression_interval(self, count: str, unit: str) -> str:
        return "'{} {}'::interval".format(count, unit)

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        return "({})::{}".format(expression, data_type)

    def execute_sql(self, sql: str):
        self.engine.execute(text(sql))