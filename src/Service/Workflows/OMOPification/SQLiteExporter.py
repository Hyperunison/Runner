from typing import List, Dict, Optional
import os
import sqlite3
import csv

from src.Service.SqlBuilder import SqlBuilder
from src.Service.Workflows.OMOPification.BaseDatabaseExporter import BaseDatabaseExporter
from src.Service.Workflows.OMOPification.LinesFilter import LinesFilter


class SQLiteExporter(BaseDatabaseExporter):
    lines_filter: LinesFilter
    file_name: str
    bin_file_name: str
    sql_builder: SqlBuilder
    cursor: sqlite3.Cursor

    def __init__(self):
        super().__init__()
        self.file_name = 'var/database.sqlite.sql'
        self.bin_file_name = 'var/database.sqlite'
        self.cursor = self.get_connection_cursor()

    def get_connection_cursor(self) -> sqlite3.Cursor:
        con = sqlite3.connect(os.path.abspath(self.file_name))
        cur = con.cursor()

        return cur

    def remove_database_file(self):
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

    def create_all_tables(self, tables: List[Dict[str, any]]):
        for table in tables:
            self.create_table(table)

    def create_table(self, table: Dict[str, any]):
        sql = self.sql_builder.build_create_table_with_field_types(
            table_name=table['tableName'],
            fields=table['columns']
        )

        self.cursor.executescript(sql)

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

        self.cursor.executescript(sql)

    def write_full_dump(self):
        if os.path.exists(self.bin_file_name):
            os.remove(self.bin_file_name)

        source_conn = sqlite3.connect(self.file_name)
        binary_conn = sqlite3.connect(self.bin_file_name)

        with binary_conn:
            source_conn.backup(binary_conn)

        source_conn.close()
        binary_conn.close()

        return self.bin_file_name

    def execute_sql(self, sql: str):
        self.cursor.executescript(sql)

    def fill_server_data_tables(self, tables: List[Dict[str, any]]):
        if self.concept_csv_path:
            self.fill_concept_table(tables)

        if self.vocabulary_csv_path:
            self.fill_vocabulary_table(tables)
