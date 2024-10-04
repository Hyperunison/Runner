from typing import List, Dict, Optional
import re
from sqlalchemy import Table, Column, MetaData, select, Select, and_, literal, text

from src.Database.BaseDatabase import BaseDatabase
from src.Database.DTO.BaseExpressionDTO import BaseExpressionDTO
from src.Database.DTO.ConditionDTO import ConditionDTO
from src.Database.DTO.ConstantExpressionDTO import ConstantExpressionDTO
from src.Database.DTO.FieldExpressionDTO import FieldExpressionDTO
from src.Database.DTO.JoinDTO import JoinDTO
from src.Database.DTO.SelectDTO import SelectDTO
from src.Database.DTO.SubqueryExpressionDTO import SubqueryExpressionDTO
from src.Database.DTO.TableFieldDTO import TableFieldDTO
from src.Database.Utils.TableNameParser import TableNameParser


class SelectRows(BaseDatabase):
    table_name_parser: TableNameParser

    def __init__(self, connection_string: str):
        self.table_name_parser = TableNameParser()
        super().__init__(connection_string)

    def execute(self, dto: SelectDTO) -> List[Dict[str, any]]:
        metadata = MetaData()
        main_table_schema_name = self.table_name_parser.get_schema_name_from_full_table_name(dto.table_name)
        table = Table(
            self.table_name_parser.get_table_name_from_full_table_name(dto.table_name),
            metadata,
            autoload_with=self.engine,
            schema=main_table_schema_name
        )
        columns = self.get_column_objects(dto)
        query = self.build_query(dto, columns, metadata)

        with self.engine.connect() as connection:
            return connection.execute(query)

    def build_query(self, dto: SelectDTO, columns: List[Column], metadata: MetaData) -> Select:
        print(columns)
        exit(0)
        query = select(columns)
        query = self.add_joins_to_query(dto, query, metadata)
        query = self.add_limit_to_query(dto, query)

        return query

    def add_joins_to_query(self, dto: SelectDTO, query: Select, metadata: MetaData) -> Select:
        for join in dto.joins:
            query = self.add_join_to_query(
                join,
                query,
                dto.table_name,
                metadata
            )

        return query

    def add_join_to_query(
            self,
            join: JoinDTO,
            query: Select,
            main_table_name: str,
            metadata: MetaData
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
        main_table_schema_name = self.table_name_parser.get_schema_name_from_full_table_name(main_table_name)

        if join.type == 'inner':
            join_table = self.get_join_table(join, main_table_schema_name, metadata)
            query = query.select_from(query.froms[0].join(join_table, join_condition))
        return query

    def add_limit_to_query(self, dto: SelectDTO, query: Select) -> Select:
        if dto.limit is not None:
            query = query.limit(dto.limit)

        return query

    def get_tables_list(self, dto: SelectDTO) -> List[Table]:
        result = []
        metadata = MetaData()
        main_table_schema_name = self.table_name_parser.get_schema_name_from_full_table_name(dto.table_name)
        table = Table(
            self.table_name_parser.get_table_name_from_full_table_name(dto.table_name),
            metadata,
            autoload_with=self.engine,
            schema=main_table_schema_name
        )
        result.append(table)

        for join in dto.joins:
            join_table = self.get_join_table(join, main_table_schema_name, metadata)
            if join_table is not None:
                result.append(join_table)

        return result

    def get_join_table(self, dto: JoinDTO, main_table_schema_name: str, metadata: MetaData) -> Optional[any]:
        if isinstance(dto.table, SubqueryExpressionDTO):
            subquery = select(
                text(dto.table.value)
            )
            if dto.alias is not None:
                subquery = subquery.alias(dto.alias)

            return subquery
        elif isinstance(dto.table, FieldExpressionDTO):
            schema = self.table_name_parser.get_schema_name_from_full_table_name(
                dto.table.value,
                main_table_schema_name
            )
            return Table(
                self.table_name_parser.get_table_name_from_full_table_name(dto.table.value),
                metadata,
                autoload_with=self.engine,
                schema=schema
            )

        return None

    def get_column_objects(self, dto: SelectDTO) -> List[Column]:
        result = []
        tables = self.get_tables_list(dto)
        for field in dto.fields:
            # table_name = self.get_table_name_from_field_name(field)
            table_field = self.table_name_parser.get_table_field_from_alias(field)

            if table_field is None:
                continue
            # print(table_field.table_name)
            real_table_name = table_field.table_name
            alias_dto = dto.get_origin_table_by_alias(real_table_name)

            if alias_dto is not None:
                real_table_name = alias_dto.table_name

            for table in tables:
                print(table.name, real_table_name)
                print('========================================')
                if table.name == real_table_name:
                    if table_field.field_name in table.c:
                        result.append(table.c[table_field.field_name])

        return result

    def build_join_condition(
            self,
            condition: ConditionDTO,
            main_table_name: str,
            metadata: MetaData
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
            metadata: MetaData
    ) -> Optional[any]:
        if isinstance(field, FieldExpressionDTO):
            table_name = self.table_name_parser.get_table_name_from_field_name(
                field.value
            )
            field_name = self.table_name_parser.get_field_name_from_field_name(
                field.value
            )
            schema_name = self.table_name_parser.get_schema_name_from_full_table_name(
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