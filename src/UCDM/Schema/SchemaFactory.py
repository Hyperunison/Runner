from src.Database.Utils.DsnParser import DsnParser
from src.UCDM.Schema.BaseSchema import BaseSchema
from src.UCDM.Schema.Labkey import Labkey
from src.UCDM.Schema.Mysql import Mysql
from src.UCDM.Schema.Postgres import Postgres


class SchemaFactory:
    dsn_parser: DsnParser

    def __init__(self):
        self.dsn_parser = DsnParser()

    def create(self, dsn: str, min_count: int) -> BaseSchema:
        type = self.dsn_parser.get_engine_type(dsn)

        if type == "postgresql":
            return Postgres(dsn, min_count)
        if type == "mysql":
            return Mysql(dsn, min_count)
        if type == "labkey":
            return Labkey(dsn, min_count)
        if type == "mssql":
            pass

        raise Exception("Unknown schema type")