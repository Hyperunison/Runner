from sqlalchemy import MetaData, Table, insert
from src.Database.BaseDatabase import BaseDatabase
from src.Database.DTO.InsertRowsDTO import InsertRowsDTO

class InsertRows(BaseDatabase):

    def execute(self, dto: InsertRowsDTO):
        metadata = MetaData()
        table = Table(dto.table_name, metadata, autoload_with=self.engine, schema='public')

        with self.engine.connect() as connection:
            with connection.begin():
                stmt = insert(table)
                for row in dto.rows:
                    connection.execute(stmt.values(row))