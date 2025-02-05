from typing import List, Dict

from src.Service.SqlBuilder import SqlBuilder
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.Workflows.OMOPification.LinesFilter import LinesFilter
from src.UCDM.DataSchema import DataSchema


class PostgresqlExporter:
    lines_filter: LinesFilter
    connection_string: str
    data_schema: DataSchema
    sql_builder: SqlBuilder

    def __init__(self, connection_string: str):
        self.lines_filter = LinesFilter()
        self.connection_string = connection_string
        self.data_schema = DataSchema(
            dsn=connection_string,
            min_count=0
        )
        self.sql_builder = SqlBuilder()

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