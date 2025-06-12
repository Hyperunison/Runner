import csv
import os
from typing import List, Dict, Optional, Any

from src.Service.SqlBuilder import SqlBuilder
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.Workflows.OMOPification.LinesFilter import LinesFilter


class BaseDatabaseExporter:
    lines_filter: LinesFilter
    sql_builder: SqlBuilder
    concept_csv_path: Optional[str] = None
    vocabulary_csv_path: Optional[str] = None

    def __init__(self):
        self.lines_filter = LinesFilter()
        self.sql_builder = SqlBuilder()

    def create_all_tables(self, tables: List[Dict[str, any]]):
        pass

    def export(
            self,
            table_name: str,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]],
            columns: List[Dict[str, str]]
    ) -> Dict[str, int]:
        return self.save_rows(
            table_name=table_name,
            ucdm=ucdm,
            fields_map=fields_map,
            columns=columns
        )

    # In case if exporter writes each table to separate file. Returns filename of dump to upload to S3
    def write_single_table_dump(self, table_name: str) -> Optional[str]:
        return None

    # In case if exporter writes full dump into a single file. Returns filename of dump to upload to S3
    def write_full_dump(self) -> Optional[str]:
        return None

    def save_rows(
            self,
            table_name: str,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]],
            columns: List[Dict[str, str]]
    ) -> Dict[str, int]:
        skip_rows: Dict[str, int] = {}
        correct_rows: List[Dict[str, any]] = []

        for row in ucdm:
            skip_reasons: List[str] = self.lines_filter.get_line_errors(row, fields_map)
            if len(skip_reasons) == 0:
                correct_row = {}

                for key, val in row.items():
                    if not key in fields_map:
                        continue
                    value = val.export_value
                    field_name = fields_map[key]['name']
                    correct_row[field_name] = value if value is not None else ''

                correct_rows.append(correct_row)
            else:
                row_str: Dict[str, str] = {}
                for k, v in row.items():
                    row_str[k] = v.export_value
                for reason, count in skip_reasons.items():
                    if not reason in skip_rows:
                        skip_rows[reason] = 0
                    skip_rows[reason] += skip_reasons[reason]

        if len(correct_rows) > 0:
            self.insert_rows(
                table_name=table_name,
                rows=correct_rows,
                fields_map=fields_map,
                columns=columns
            )

        return skip_rows

    def insert_rows(
        self,
        table_name: str,
        rows: List[Dict[str, str]],
        fields_map: Dict[str, Dict[str, str]],
        columns: List[Dict[str, str]]
    ):
        raise NotImplementedError()

    def transform_rows_by_fields_map(
            self,
            rows: List[Dict[str, any]],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[Dict[str, any]]:
        result: List[Dict[str, any]] = []
        database_fields_map = self.transform_fields_map_to_database_fields_map(fields_map)

        for row in rows:
            result_row: Dict[str, any] = {}

            for key, val in row.items():
                result_row[database_fields_map[key]] = val

            result.append(result_row)

        return result

    def transform_fields_map_to_database_fields_map(
            self,
            fields_map: Dict[str, Dict[str, str]]
    ) -> Dict[str, str]:
        result: Dict[str, str] = {}

        for field, value in fields_map.items():
            result[value['name']] = value['name']

        return result

    def fill_server_data_tables(self, tables: List[Dict[str, any]]):
        pass

    def execute_sql(self, sql: str):
        pass

    def read_csv_file(self, file_name: str) -> List[Dict[str, any]]:
        with open(file_name, 'r', newline='') as file:
            result: List[Dict[str, any]] = []
            reader = csv.reader(file, delimiter=',', quotechar='"')
            try:
                header = next(reader)
            except StopIteration:
                return result

            for row in reader:
                result_row: Dict[str, any] = {}
                for index, value in enumerate(row):
                    result_row[header[index]] = value

                result.append(result_row)

            return result

    def get_columns(self, table_name: str, tables: List[Dict[str, any]]) -> List[Dict[str, any]]:
        for table in tables:
            if table['tableName'] == table_name:
                return table['columns']

        return None

    def fill_concept_table(self, tables: List[Dict[str, any]]):
        table_name = 'concept'
        rows = self.read_csv_file(os.path.abspath(self.concept_csv_path))
        self.fill_table(
            rows=rows,
            table_name=table_name,
            tables=tables
        )

    def fill_vocabulary_table(self, tables: List[Dict[str, any]]):
        table_name = 'vocabulary'
        rows = self.read_csv_file(os.path.abspath(self.vocabulary_csv_path))
        self.fill_table(
            rows=rows,
            table_name=table_name,
            tables=tables
        )

    def fill_table(self, rows: List[Dict[str, any]], table_name: str, tables: List[Dict[str, any]]):
        columns = self.get_columns(table_name=table_name, tables=tables)

        if columns is None:
            return

        sql = self.sql_builder.build_insert(
            table_name=table_name,
            rows=rows,
            columns=columns
        )

        self.execute_sql(sql)
