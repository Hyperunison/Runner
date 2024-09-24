from typing import List, Dict

from src.Service.SqlBuilder import SqlBuilder
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.Workflows.OMOPification.LinesFilter import LinesFilter
from src.UCDM.DataSchema import DataSchema


class PostgresqlExporter:
    lines_filter: LinesFilter
    connection_string: str
    table_name: str
    data_schema: DataSchema
    sql_builder: SqlBuilder

    def __init__(self, connection_string: str, table_name: str):
        self.lines_filter = LinesFilter()
        self.connection_string = connection_string
        self.table_name = table_name
        self.data_schema = DataSchema(
            dsn=connection_string,
            schema="postgres",
            min_count=0
        )

    def export(
            self,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        self.build_database_structure(
            ucdm=ucdm,
            fields_map=fields_map
        )

        return self.save_rows(
            ucdm=ucdm,
            fields_map=fields_map
        )

    def build_database_structure(
            self,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]]
    ):
        field_names = self.get_field_names(ucdm, fields_map)
        if len(fields_map) == 0:
            return

        sql = self.sql_builder.build_create_table(table_name=self.table_name, field_names=field_names)
        self.data_schema.execute_sql(sql=sql)

    def save_rows(
            self,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        skip_rows: List[str] = []
        correct_rows: List[Dict[str, str]] = []

        for row in ucdm:
            skip_reasons: List[str] = self.lines_filter.get_line_errors(row, fields_map)
            if len(skip_reasons) == 0:
                pass
            else:
                row_str: Dict[str, str] = {}
                for k, v in row.items():
                    row_str[k] = v.export_value
                skip_rows.append("Skip row as [{}]. Row={}".format(", ".join(skip_reasons), str(row_str)))

        if len(correct_rows) > 0:
            self.insert_rows(correct_rows)

        return skip_rows

    def insert_rows(
            self,
            rows: List[Dict[str, str]]
    ):
        sql = self.sql_builder.build_insert(table_name=self.table_name, rows=rows)
        self.data_schema.execute_sql(sql=sql)

    def get_field_names(
            self,
            rows: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        result: List[str] = []

        for row in rows:
            skip_reasons: List[str] = self.lines_filter.get_line_errors(row, fields_map)
            if len(skip_reasons) == 0:
                for key, val in row.items():
                    field_name = fields_map[key]['name']
                    result.append(field_name)
                break

        return result