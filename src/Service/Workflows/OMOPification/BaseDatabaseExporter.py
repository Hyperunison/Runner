from typing import List, Dict, Optional

from src.Service.SqlBuilder import SqlBuilder
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.Workflows.OMOPification.LinesFilter import LinesFilter


class BaseDatabaseExporter:
    lines_filter: LinesFilter
    sql_builder: SqlBuilder

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
    ) -> List[str]:
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
    ) -> List[str]:
        skip_rows: List[str] = []
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
                skip_rows.append("Skip row as [{}]. Row={}".format(", ".join(skip_reasons), str(row_str)))

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

