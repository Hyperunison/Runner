from typing import List, Dict

class SqlBuilder:

    def build_insert(self, table_name: str, rows: List[Dict[str, str]]) -> str:
        columns = rows[0].keys()
        columns_part = ", ".join([f"`{col}`" for col in columns])
        values_part = ", ".join([
            "(" + ", ".join([f"'{value}'" for value in row.values()]) + ")"
            for row in rows
        ])

        sql_query = f"INSERT INTO `{table_name}` ({columns_part}) VALUES {values_part};"

        return sql_query

    def build_create_table(self, table_name: str, field_names: List[str]) -> str:
        columns_definition = ", ".join([f"`{field}` VARCHAR(512)" for field in field_names])
        create_table_query = f"CREATE TABLE `{table_name}` ({columns_definition});"

        return create_table_query