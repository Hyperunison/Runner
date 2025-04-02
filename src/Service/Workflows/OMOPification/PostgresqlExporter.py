from typing import List, Dict

from src.Service.SqlBuilder import SqlBuilder
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.Workflows.OMOPification.BaseDatabaseExporter import BaseDatabaseExporter
from src.Service.Workflows.OMOPification.LinesFilter import LinesFilter
from src.UCDM.DataSchema import DataSchema


class PostgresqlExporter(BaseDatabaseExporter):
    connection_string: str
    data_schema: DataSchema

    def __init__(self, connection_string: str):
        super().__init__()
        self.connection_string = connection_string
        self.data_schema = DataSchema(
            dsn=connection_string,
            min_count=0
        )

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
        self.data_schema.execute_sql(sql)

    def get_field_names(
            self,
            rows: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        result: List[str] = []

        for row in rows:
            skip_reasons: List[str] = self.lines_filter.get_line_errors(row, fields_map)
            if len(skip_reasons) == 0:
                result = []
                for key, val in row.items():
                    field_name = fields_map[key]['name']
                    result.append(field_name)
                break

        return list(set(result))

    def create_all_tables(self, tables: List[Dict[str, any]]):
        for table in tables:
            self.create_table(table)

    def create_table(self, table: Dict[str, any]):
        sql = self.sql_builder.build_create_table_with_field_types(
            table_name=table['tableName'],
            fields=table['columns']
        )
        self.data_schema.execute_sql(sql)

    def fill_server_data_tables(self, tables: List[Dict[str, any]]):
        if self.concept_csv_path:
            self.fill_concept_table(tables)

        if self.vocabulary_csv_path:
            self.fill_vocabulary_table(tables)

    def execute_sql(self, sql: str):
        self.data_schema.execute_sql(sql)