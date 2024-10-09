from src.Database.Converters.ConvertRawSql import ConvertRawSql
from src.Database.Utils.DsnParser import DsnParser
from sqlalchemy import create_engine
from sqlalchemy import text
from decimal import Decimal
import datetime

class EngineFacade:
    dsn: str
    dsn_parser: DsnParser
    converter: ConvertRawSql

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.dsn_parser = DsnParser()
        self.converter = ConvertRawSql()

    def fetch(self, query: str):
        database_type = self.dsn_parser.get_engine_type(self.dsn)
        converted_sql = self.converter.convert_raw_sql(query, database_type)

        if database_type == 'postgresql':
            engine = create_engine(self.dsn, isolation_level="AUTOCOMMIT").connect()
            result = engine.execute(text(converted_sql)).mappings().all()
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

        elif database_type == 'mysql':
            raise ValueError("MySQL database type not supported")
        elif database_type == 'mssql':
            raise ValueError("MSSQL database type not supported")

    def execute(self, query: str):
        database_type = self.dsn_parser.get_engine_type(self.dsn)
        converted_sql = self.converter.convert_raw_sql(query, database_type)

        if database_type == 'postgresql':
            engine = create_engine(self.dsn, isolation_level="AUTOCOMMIT").connect()
            engine.execute(text(converted_sql))
        elif database_type == 'mysql':
            raise ValueError("MySQL database type not supported")
        elif database_type == 'mssql':
            raise ValueError("MSSQL database type not supported")