import logging
from typing import List, Dict, Tuple
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.errors import UndefinedFunction, UndefinedTable
from src.UCDM.Schema.BaseSchema import BaseSchema
from src.UCDM.TableStat import TableStat
import datetime

Base = declarative_base()

class Postgres(BaseSchema):
    type = 'postgres'
    dsn = ''

    def __init__(self, dsn: str, min_count: int):
        self.dsn = dsn
        self.engine = create_engine(dsn).connect()
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

    def get_table_column_stats(self, table_name: str, column_name: str) -> TableStat:
        sql = "SELECT count(distinct \"{v}\") as unique_count from {table}".format(v=column_name, table=table_name)
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
            row = self.fetch_row(sql)

            min_value = row['min_value']
            max_value = row['max_value']
            avg_value = row['avg_value']
            median50_value = self.get_median(table_name, column_name, min_value, max_value)
            median25_value = self.get_median(table_name, column_name, min_value, median50_value)
            median12_value = self.get_median(table_name, column_name, min_value, median25_value)
            median37_value = self.get_median(table_name, column_name, median25_value, median50_value)
            median75_value = self.get_median(table_name, column_name, median50_value, max_value)
            median63_value = self.get_median(table_name, column_name, median50_value, median75_value)
            median88_value = self.get_median(table_name, column_name, median75_value, max_value)
            values_counts = []
        except ProgrammingError as e:
            logging.debug("Can't get min/max values for {}.{}".format(table_name, column_name))
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

        sql = "SELECT \"{}\" as value, count(*) as cnt from {} WHERE NOT \"{}\" IS NULL GROUP BY 1 HAVING COUNT(*) > {} ORDER BY 1 DESC LIMIT 10".format(
            column_name, table_name, column_name, self.min_count)
        logging.debug(sql)
        values_counts = self.fetch_all(sql)
        logging.info("Frequent values counts for {}.{}: {}".format(table_name, column_name, (values_counts)))
        nulls_count = self.fetch_row("SELECT COUNT(*) as cnt FROM {} WHERE \"{}\" is null".format(table_name, column_name))['cnt']

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

    def get_median(self, table: str, column: str, min, max):
        if min is None or max is None:
            return None
        sql = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY {column}) as median FROM {table} WHERE {column} > {min} and {column} < {max}".format(column='"'+column+'"', table=table, min=min, max=max)
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

        return result

    def rollback(self):
        self.engine.rollback()

    def reconnect(self):
        self.engine.close()
        self.engine = create_engine(self.dsn).connect()
