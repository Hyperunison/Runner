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