from typing import List, Dict

class SqlBuilder:

    def build_insert(self, table_name: str, rows: List[Dict[str, str]], columns: List[Dict[str, str]]) -> str:
        columns_list = rows[0].keys()
        columns_part = ", ".join([f"{col}" for col in columns_list])
        transformed_rows = self.transform_rows_with_types(rows, columns)
        values_part = ", ".join([
            "(" + ", ".join([f"{value}" for value in row.values()]) + ")"
            for row in transformed_rows
        ])

        sql_query = f"INSERT INTO {table_name} ({columns_part}) VALUES {values_part};"

        return sql_query

    def build_create_table(self, table_name: str, field_names: List[str]) -> str:
        columns_definition = ", ".join([f"{field} VARCHAR(512)" for field in field_names])
        create_table_query = f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({columns_definition});"

        return create_table_query

    def build_create_table_with_field_types(self, table_name: str, fields: List[Dict[str, any]]) -> str:
        create_statement = f"DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" ("
        field_definitions = []

        for field in fields:
            field_type = self.get_field_type(field['type'])
            field_def = f"\"{field['name']}\" {field_type} NULL"

            # if field['required']:
            #     field_def += " NOT NULL"

            field_definitions.append(field_def)

        create_statement += ", ".join(field_definitions)
        create_statement += ");"

        return create_statement

    def get_field_type(self, origin_type: str) -> str:
        if origin_type.lower() == "datetime":
            return "timestamp"

        if origin_type.lower() == "varchar(max)":
            return "text"

        if origin_type.lower() == "integer":
            return "bigint"

        return origin_type.lower()

    def transform_rows_with_types(
            self,
            rows: List[Dict[str, str]],
            columns: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        result: List[Dict[str, str]] = []

        for row in rows:
            result_item = {}
            for key, value in row.items():
                result_item[key] = self.build_row_field_value(value, key, columns)
            result.append(result_item)

        return result

    def build_row_field_value(self, value: str, field_name:str, columns: List[Dict[str, str]]) -> str:
        for column in columns:
            if column['name'] == field_name:
                if column['type'] == 'datetime' and not column['required'] and value == '':
                    return 'null'
                if column['type'] == 'date' and not column['required'] and value == '':
                    return 'null'
                if column['type'] == 'float' and not column['required'] and value == '':
                    return 'null'
                if column['type'] == 'integer' and not column['required'] and value == '':
                    return 'null'

        return "'" + value + "'"