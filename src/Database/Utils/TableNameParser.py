import re
from typing import Optional
from src.Database.DTO.TableFieldDTO import TableFieldDTO


class TableNameParser:

    def get_table_field_from_alias(self, alias: str) -> Optional[TableFieldDTO]:
        match = re.match(r"(\w+)\.(\w+)(?:::(\w+))?(?:\s+as\s+(\w+\.\w+))?", alias)
        if match:
            return TableFieldDTO(
                table_name=match.group(1),
                field_name=match.group(2),
            )

        return None

    def get_table_name_from_full_field_name(self, field_name: str) -> str:
        parts = field_name.split('.')
        if len(parts) == 2:
            # Case: table_name.field
            return parts[0]  # table_name
        elif len(parts) == 3:
            # Case: schema.table_name.field
            return f"{parts[0]}.{parts[1]}"  # schema.table_name
        else:
            raise ValueError("Unexpected format")

    def get_field_name_from_field_name(self, field_name: str) -> str:
        parts = field_name.split('.')
        if len(parts) == 2 or len(parts) == 3:
            return parts[-1]
        else:
            raise ValueError("Unexpected format")

    def get_table_name_from_field_name(self, field_name: str) -> str:
        parts = field_name.split('.')
        if len(parts) == 2:
            return parts[0]
        elif len(parts) == 3:
            return parts[1]
        else:
            raise ValueError("Unexpected format")

    def get_table_name_from_full_table_name(self, table_name: str):
        parts = table_name.split('.')
        if len(parts) == 1:
            return parts
        elif len(parts) == 2:
            return parts[1]
        else:
            raise ValueError("Unexpected format")

    def get_schema_name_from_full_table_name(self, table_name: str, default_schema_name: str = None) -> str:
        parts = table_name.split('.')
        if len(parts) == 2:
            return parts[0]
        else:
            if default_schema_name is not None:
                return default_schema_name
            else:
                raise ValueError("Unexpected format")