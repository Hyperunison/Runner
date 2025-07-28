import decimal
import logging
from decimal import Decimal
from typing import List, Dict, Tuple, Optional
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.errors import UndefinedFunction, UndefinedTable

from src.Helpers.SQLWithParameters import SQLWithParameters
from src.UCDM.Exception.NonNumericField import NonNumericField
from src.UCDM.Schema.BaseSchema import BaseSchema
from src.UCDM.TableStat import TableStat
import datetime
from psycopg2 import sql

Base = declarative_base()


class Database(BaseSchema):
    type = 'postgres'
    dsn = ''

    def __init__(self, dsn: str, min_count: int):
        self.dsn = dsn
        if self.engine is None:
            self.engine = create_engine(dsn, isolation_level="AUTOCOMMIT").connect()
        super().__init__(dsn, min_count)


    def fetch_row_deprecated(self, sql: str) -> Dict:
        result = self.engine.execute(text(sql)).mappings().all()
        result = [dict(row) for row in result]

        return result[0]

    def fetch_all_deprecated(self, sql: str):
        result_intermediate = self.engine.execute(text(sql)).fetchall()
        columns = [col[0] for col in self.engine.execute(text(sql)).cursor.description]
        result = [dict(zip(columns, row)) for row in result_intermediate]

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

    def fetch_all(self, query: SQLWithParameters):
        result_intermediate = self.engine.execute(text(query.sql), query.parameters).fetchall()
        columns = [col[0] for col in self.engine.execute(text(query.sql), query.parameters).cursor.description]
        result = [dict(zip(columns, row)) for row in result_intermediate]

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

    def fetch_row(self, query: SQLWithParameters) -> Dict:
        result = self.engine.execute(text(query.sql), query.parameters).mappings().all()
        result = [dict(row) for row in result]

        return result[0]

    def execute_sql(self, query: SQLWithParameters) -> str:
        self.engine.execute(text(query.sql), query.parameters)

    def rollback(self):
        self.engine.rollback()

    def reconnect(self):
        self.engine.close()
        self.engine = create_engine(self.dsn).connect()

    def sql_expression_interval(self, count: str, unit: str) -> str:
        return "'{} {}'::interval".format(count, unit)

    def sql_expression_cast_data_type(self, expression: str, data_type: str) -> str:
        return "({})::{}".format(expression, data_type)

    def execute_sql_deprecated(self, sql: str):
        self.engine.execute(text(sql))

    def get_table_column_stats(self, table_name: str, column_name: str, cte: str) -> TableStat:
        with_cte_label = 'with CTE' if cte else ''
        try:
            unique_count = self.get_unique_values_count(table_name, column_name, cte)
        except ProgrammingError as e:
            self.engine.rollback()
            if isinstance(e.orig, UndefinedTable):
                return self.build_abandoned_table_column_stat(table_name, column_name)
            else:
                raise e

        try:
            row = self.get_min_max_avg_value(table_name, column_name, cte)
            min_value = row['min_value']
            max_value = row['max_value']
            avg_value = row['avg_value']

            if not self.is_numeric(min_value) or not self.is_numeric(max_value) or not self.is_numeric(avg_value):
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
        except NonNumericField as e:
            logging.debug("Can't get min/max values for {}.{} {}".format(table_name, column_name, with_cte_label))
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

        sql = self.get_values_count_query(table_name, column_name, cte)
        sql = self.wrap_sql_by_cte(sql, table_name, cte)
        logging.debug(sql)
        values_counts = self.fetch_all_deprecated(sql)
        logging.info("Frequent values counts for {}.{} {}: {}".format(
            table_name,
            column_name,
            with_cte_label,
            (values_counts)
        ))
        nulls_count_sql = self.get_nulls_count_query(table_name, column_name, cte)
        nulls_count = self.fetch_row_deprecated(nulls_count_sql)['cnt']

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

    def get_min_max_avg_value(self, table_name: str, column_name: str, cte: str) -> Dict[str, any]:
        sql = self.get_min_max_avg_value_query(table_name, column_name, cte)
        row = self.fetch_row_deprecated(sql)

        return row

    def get_median(self, table: str, column: str, min, max, cte: str):
        if min is None or max is None:
            return None
        sql = self.get_median_query(table, column, min, max, cte)
        sql = self.wrap_sql_by_cte(sql, table, cte)

        return self.fetch_row_deprecated(sql)['median']

    def get_median_query(self, table: str, column: str, min, max, cte: str) -> str:
        raise Exception("Not implemented")

    def build_abandoned_table_column_stat(self, table_name: str, column_name: str) -> TableStat:
        stat = TableStat()
        stat.table_name = table_name
        stat.column_name = column_name
        stat.abandoned = True
        return stat

    def get_unique_values_count_query(self, table_name: str, column_name: str) -> str:
        raise Exception("Not implemented")

    def get_unique_values_count(self, table_name: str, column_name: str, cte: str) -> int:
        sql = self.get_unique_values_count_query(table_name, column_name)
        sql = self.wrap_sql_by_cte(sql, table_name, cte)
        row = self.fetch_row_deprecated(sql)
        return row['unique_count']

    def get_min_max_avg_value_query(self, table_name: str, column_name: str, cte: str) -> str:
        raise Exception("Not implemented")

    def get_values_count_query(self, table_name: str, column_name: str, cte: str) -> str:
        raise Exception("Not implemented")

    def get_nulls_count_query(self, table_name: str, column_name: str, cte: str) -> str:
        raise Exception("Not implemented")

    def is_numeric(self, value) -> bool:
        return isinstance(value, (int, float, complex, decimal.Decimal))

    def escape_table_name(self, table_name: str) -> str:
        result: List[str] = []
        chunks = table_name.split('.')
        for chunk in chunks:
            result.append(self.engine.dialect.identifier_preparer.quote(chunk))

        return '.'.join(result)

    def escape_column_name(self, table_name: str) -> str:
        return self.engine.dialect.identifier_preparer.quote(table_name)

