from typing import Dict, List, Optional

from src.Database.DTO.FieldExpressionDTO import FieldExpressionDTO
from src.Database.DTO.JoinDTO import JoinDTO
from src.Database.DTO.SelectDTO import SelectDTO
from src.Database.DTO.SubqueryExpressionDTO import SubqueryExpressionDTO
from src.Database.DTO.TableAliasDTO import TableAliasDTO
from src.Database.Transformers.ConditionExpressionParser import ConditionExpressionParser
from src.Database.Transformers.SqlExpressionParser import SqlExpressionParser
from src.Database.Transformers.SubqueryExpressionParser import SubqueryExpressionParser
from src.Database.Utils.TableNameParser import TableNameParser
from src.Message.partial.CohortDefinition import CohortDefinition

class MessageQueryToDtoTransformer:
    variable_mapping: Dict[str, str] = {}
    table_name_parser: TableNameParser

    def __init__(self):
        self.table_name_parser = TableNameParser()

    def transform(self, message: CohortDefinition) -> SelectDTO:
        self.init_variable_mapping(message.fields)
        result = SelectDTO(
            table_name=self.get_table_name(message),
        )
        result.fields = self.get_fields_list(message)
        result = self.fill_joins(message, result)
        result = self.fill_where(message, result)
        result = self.fill_limit(message, result)

        return result

    def fill_joins(self, message: CohortDefinition, dto: SelectDTO) -> SelectDTO:
        expression_parser = ConditionExpressionParser()
        sql_expression_parser = SqlExpressionParser()
        subquery_expression_parser = SubqueryExpressionParser()

        for join in message.joins:
            table_expression = sql_expression_parser.parse(join['table'])
            alias = join['alias']
            if isinstance(table_expression, SubqueryExpressionDTO) and alias is None:
                alias = subquery_expression_parser.get_subquery_alias(table_expression)

            if isinstance(table_expression, FieldExpressionDTO) and alias is not None:
                dto = self.add_alias_to_table_aliases(
                    dto=dto,
                    expression=table_expression,
                    alias=alias
                )

            join_dto = JoinDTO(
                table=table_expression,
                alias=alias,
            )
            condition = expression_parser.parse(join['on'])

            if condition is not None:
                join_dto.conditions.append(condition)
                dto.joins.append(join_dto)

        return dto

    def add_alias_to_table_aliases(self, dto: SelectDTO, expression: FieldExpressionDTO, alias: str) -> SelectDTO:
        table_name = self.table_name_parser.get_table_name_from_full_table_name(
            expression.value
        )
        schema_name = self.table_name_parser.get_schema_name_from_full_table_name(
            expression.value
        )
        alias_dto = TableAliasDTO(
            alias=alias,
            table_name=table_name,
            schema_name=schema_name
        )
        dto.table_aliases.append(alias_dto)

        return dto

    def fill_where(self, message: CohortDefinition, dto: SelectDTO) -> SelectDTO:
        for where in message.where:
            pass
        return dto

    def fill_limit(self, message: CohortDefinition, dto: SelectDTO) -> SelectDTO:
        if message.limit is not None:
            dto.limit = message.limit

        return dto

    def get_table_name(self, message: CohortDefinition) -> str:
        return message.participant_table

    def get_fields_list(self, message: CohortDefinition) -> List[str]:
        result = []

        for exp in message.export:
            result_item = self.build_fields_list_item(exp)

            if result_item is not None:
                result.append(result_item)

        return result

    def build_fields_list_item(self, export) -> Optional[str]:
        if 'as' in export:
            if export['type'] == 'variable':
                alias = export['as']
                field_name = self.convert_var_name_with_mapping(alias)
                return "{} as {}".format(field_name, alias)

        return None

    def init_variable_mapping(self, fields):
        if type(fields).__name__ == "dict":
            for ucdm in fields.keys():
                self.variable_mapping[ucdm] = fields[ucdm]

    def convert_var_name_with_mapping(self, var: str) -> str:
        if not self.variable_mapping.__contains__(var):
            raise Exception("Unknown object var {}".format(var))

        return self.variable_mapping[var]