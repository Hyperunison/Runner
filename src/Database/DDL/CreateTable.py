from typing import List, Optional
import re

from src.Database.BaseDatabase import BaseDatabase
from src.Database.DTO.CreateTableColumnDTO import CreateTableColumnDTO
from src.Database.DTO.CreateTableDTO import CreateTableDTO
from sqlalchemy import Table, Column, MetaData, Integer, String, Text, Date, DateTime, Float

class CreateTable(BaseDatabase):

    def execute(self, table: CreateTableDTO):
        metadata = MetaData()
        metadata.bind = self.engine
        columns: List[Column] = []

        for column in table.columns:
            columns.append(self.create_table_column(column))

        table = Table(
            table.table_name,
            metadata,
            *columns,
            schema = "public"
        )
        with self.engine.connect() as connection:
            with connection.begin():
                metadata.create_all(connection, checkfirst=False)

    def create_table_column(self, column: CreateTableColumnDTO) -> Column:
        column_type = self.get_column_type_object(column.type)
        
        return Column(
            column.name,
            column_type,
            nullable=column.nullable,
            primary_key=column.primary_key
        )

    def get_column_type_object(self, column_type: str) -> any:
        if column_type == "integer":
            return Integer
        if column_type.startswith("varchar"):
            length = self.get_string_length(column_type)

            if length:
                return String(length=length)
            return String
        if column_type == "text":
            return Text
        if column_type == "date":
            return Date
        if column_type == "datetime":
            return DateTime
        if column_type == "float":
            return Float
        return None

    def get_string_length(self, column_type: str) -> Optional[int]:
        match = re.search(r'\d+', column_type)
        if match:
            length = int(match.group())
            return length

        return None
