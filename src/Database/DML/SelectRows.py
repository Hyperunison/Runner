from typing import List, Dict, Optional
from sqlalchemy import Table, Column, MetaData, select, Select, and_, literal

from src.Database.BaseDatabase import BaseDatabase
from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO
from src.Database.DTO.ConditionDTO import ConditionDTO
from src.Database.DTO.ConstantExpressionDTO import ConstantExpressionDTO
from src.Database.DTO.FieldExpressionDTO import FieldExpressionDTO
from src.Database.DTO.JoinDTO import JoinDTO
from src.Database.DTO.SelectDTO import SelectDTO
from src.Database.DTO.SubqueryExpressionDTO import SubqueryExpressionDTO


class SelectRows(BaseDatabase):

    def execute(self, dto: SelectDTO) -> List[Dict[str, any]]:
        metadata = MetaData()
        main_table_schema_name = self.get_schema_name_from_full_table_name(dto.table_name)
        table = Table(
            self.get_table_name_from_full_table_name(dto.table_name),
            metadata,
            autoload_with=self.engine,
            schema=main_table_schema_name
        )
        columns = self.get_column_objects(dto)
        query = self.build_query(dto, columns)

        with self.engine.connect() as connection:
            return connection.execute(query)

    def build_query(self, dto: SelectDTO, columns: List[Column]) -> Select:
        query = select(columns)
        query = self.add_joins_to_query(dto, query)
        query = self.add_limit_to_query(dto, query)

        return query

    def add_joins_to_query(self, dto: SelectDTO, query: Select) -> Select:
        for join in dto.joins:
            query = self.add_join_to_query(join, query)

        return query

    def add_join_to_query(
            self,
            join: JoinDTO,
            query: Select,
            main_table_name: str,
            metadata: Metadata
    ) -> Select:
        conditions = []
        for condition in join.conditions:
            conditions.append(
                self.build_join_condition(
                    condition=condition,
                    main_table_name=main_table_name,
                    metadata=metadata
                )
            )
        join_condition = and_(*conditions)

        if join.type == 'inner':
            query = query.select_from(query.froms[0].join(join_table, join_condition))
        return query

    def add_limit_to_query(self, dto: SelectDTO, query: Select) -> Select:
        if dto.limit is not None:
            query = query.limit(dto.limit)

        return query

    def get_tables_list(self, dto: SelectDTO) -> List[Table]:
        result = []
        metadata = MetaData()
        main_table_schema_name = self.get_schema_name_from_full_table_name(dto.table_name)
        table = Table(
            self.get_table_name_from_full_table_name(dto.table_name),
            metadata,
            autoload_with=self.engine,
            schema=main_table_schema_name
        )
        result.append(table)

        for join in dto.joins:
            schema = self.get_schema_name_from_full_table_name(
                join.table_name,
                main_table_schema_name
            )
            join_table = Table(
                self.get_table_name_from_full_table_name(join.table_name),
                metadata,
                autoload_with=self.engine,
                schema=schema
            )
            result.append(join_table)

        return result

    def get_column_objects(self, dto: SelectDTO) -> List[Column]:
        result = []
        tables = self.get_tables_list(dto)
        for field in dto.fields:
            table_name = self.get_table_name_from_field_name(field)
            for table in tables:
                if table.name == table_name:
                    if field in table.c:
                        result.append(table.c[field])

        return result

    def get_schema_name_from_full_table_name(self, table_name: str, default_schema_name: str = None) -> str:
        parts = table_name.split('.')
        if len(parts) == 2:
            return parts[0]
        else:
            if default_schema_name is not None:
                return default_schema_name
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

    def get_table_name_from_field_name(self, field_name: str) -> str:
        parts = field_name.split('.')
        if len(parts) == 2:
            return parts[0]
        elif len(parts) == 3:
            return parts[1]
        else:
            raise ValueError("Unexpected format")

    def get_field_name_from_field_name(self, field_name: str) -> str:
        parts = field_name.split('.')
        if len(parts) == 2 or len(parts) == 3:
            return parts[-1]
        else:
            raise ValueError("Unexpected format")

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

    def build_join_condition(
            self,
            condition: ConditionDTO,
            main_table_name: str,
            metadata: Metadata
    ):
        left_field = self.build_join_field(condition.left, main_table_name, metadata)
        right_field = self.build_join_field(condition.right, main_table_name, metadata)

        if condition.operation == '==':
            return left_field == right_field
        elif condition.operation == '!=':
            return left_field != right_field
        elif condition.operation == '<':
            return left_field < right_field
        elif condition.operation == '<=':
            return left_field <= right_field
        elif condition.operation == '>':
            return left_field > right_field
        elif condition.operation == '>=':
            return left_field >= right_field
        else:
            raise ValueError(f"Unsupported operation: {condition.operation}")

    def build_join_field(
            self,
            field: BaseExpressionDTO,
            main_table_name: str,
            metadata: Metadata
    ) -> Optional[any]:
        if isinstance(field, FieldExpressionDTO):
            table_name = self.get_table_name_from_field_name(
                field.value
            )
            field_name = self.get_field_name_from_field_name(
                field.value
            )
            schema_name = self.get_schema_name_from_full_table_name(
                main_table_name,
            )
            return Table(
                table_name,
                metadata,
                autoload_with=self.engine,
                schema=schema_name
            ).c[field_name]
        elif isinstance(field, ConstantExpressionDTO):
            return literal(field.value)
        elif isinstance(field, SubqueryExpressionDTO):
            return select([literal(field.value)])

        return None