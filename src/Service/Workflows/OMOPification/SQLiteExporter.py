from typing import List, Dict
import os

from src.Service.SqlBuilder import SqlBuilder
from src.Service.Workflows.OMOPification.BaseDatabaseExporter import BaseDatabaseExporter
from src.Service.Workflows.OMOPification.LinesFilter import LinesFilter


class SQLiteExporter(BaseDatabaseExporter):
    lines_filter: LinesFilter
    file_name: str
    sql_builder: SqlBuilder

    def __init__(self):
        super().__init__()
        self.file_name = 'var/database.sqlite'

    def create_all_tables(self, tables: List[Dict[str, any]]):
        for table in tables:
            self.create_table(table)

    def create_table(self, table: Dict[str, any]):
        sql = self.sql_builder.build_create_table_with_field_types(
            table_name=table['tableName'],
            fields=table['columns']
        )

        if os.path.exists(self.file_name):
            os.remove(self.file_name)

        file = open(self.file_name, 'w')
        file.write(sql)
        file.close()

    def insert_rows(
        self,
        table_name: str,
        rows: List[Dict[str, str]],
        fields_map: Dict[str, Dict[str, str]],
        columns: List[Dict[str, str]]
    ):
        database_rows = self.transform_rows_by_fields_map(rows, fields_map)
        sql = self.sql_builder.build_insert(
            table_name=table_name,
            rows=database_rows,
            columns=columns
        )
        file = open(self.file_name, 'a')
        file.write(sql)
        file.close()