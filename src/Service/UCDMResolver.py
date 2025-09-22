from typing import List, Dict, Tuple
import logging
from sqlalchemy.exc import ProgrammingError

from src.Service.DTO.UCDMResult import UCDMResult
from src.UCDM.DataSchema import DataSchema
from src.Service.Workflows.SerialGenerator import SerialGenerator
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.UCDMConvertedField import UCDMConvertedField


def log_error(name_origin, field, value):
    logging.warning("Value '{value}' is unmapped in the field '{name_origin}'".format(name_origin=name_origin, field=field, value=value))

class UCDMResolver:
    schema: DataSchema
    ucdm_mapping_resolver: UCDMMappingResolver

    def __init__(self, schema: DataSchema, ucdm_mapping_resolver: UCDMMappingResolver):
        self.schema = schema
        self.ucdm_mapping_resolver = ucdm_mapping_resolver

    def get_ucdm_result(
            self,
            sql_final: str,
            str_to_int: StrToIntGenerator,
            fields_map: Dict[str, Dict[str, str]],
            automation_strategies_map: Dict[str, Dict[str, str]],
    ) -> UCDMResult:
        try:
            mapping_index = self.build_mapping_index()
            result = self.schema.fetch_all(sql_final)
            result = self.normalize(result)
            ucdm_result_lines = self.convert_to_ucdm(result, mapping_index, str_to_int, fields_map, automation_strategies_map)

            result = UCDMResult()
            result.lines = self.remove_traceability_fields(ucdm_result_lines)
            result.traceability = self.extract_traceability_lines(ucdm_result_lines)

            return result
        except ProgrammingError as e:
            logging.error("SQL query error: {}".format(e.orig))

    def remove_traceability_fields(self, ucdm_lines: List[Dict[str, UCDMConvertedField]]) -> List[Dict[str, UCDMConvertedField]]:
        result: List[Dict[str, UCDMConvertedField]] = []

        for ucdm_line in ucdm_lines:
            result_item: Dict[str, UCDMConvertedField] = {}

            for key in ucdm_line.keys():
                if key.startswith("c.__unison_audit__"):
                    continue

                result_item[key] = ucdm_line[key]

            result.append(result_item)

        return result

    def extract_traceability_lines(self, ucdm_lines: List[Dict[str, UCDMConvertedField]]) -> List[Dict[str, str]]:
        result: List[Dict[str, str]] = []

        for ucdm_line in ucdm_lines:
            if not self.does_line_have_traceability(ucdm_line):
                continue

            result.append(self.get_traceability_from_line(ucdm_line))

        return result

    def does_line_have_traceability(self, ucdm_line: Dict[str, UCDMConvertedField]) -> bool:
        for key in ucdm_line.keys():
            if key.startswith("c.__unison_audit__"):
                return True

        return False

    def get_traceability_from_line(self, ucdm_line: Dict[str, UCDMConvertedField]) -> Dict[str, str]:
        result: Dict[str, str] = {}

        for key in ucdm_line.keys():
            if not key.startswith("c.__unison_audit__"):
                continue

            result_key = key.replace("c.__unison_audit__", "")
            result[result_key] = ucdm_line[key].export_value

        return result

    def normalize(self, input_list: List[Dict[str, any]]) -> List[Dict[str, str]]:
        result: List[Dict[str, str]] = []
        for row in input_list:
            row_result: Dict[str, str] = {}
            for k, v in row.items():
                if isinstance(v, bool):
                    value = str(int(v))
                else:
                    value = str(v) if v is not None else ''
                row_result[k] = value
            result.append(row_result)
        return result

    def build_mapping_index(self) -> Dict[str, Dict[str, List[Tuple[str, str, str]]]]:
        return self.ucdm_mapping_resolver.transform_mapping_to_resolve_result()

    def convert_to_ucdm(
            self,
            result: List[Dict[str, str]],
            mapping_index: Dict[str, Dict[str, List[Tuple[str, str, str]]]],
            str_to_int: StrToIntGenerator,
            fields_map: Dict[str, Dict[str, str]],
            automation_strategies_map: Dict[str, Dict[str, str]],
    ) -> List[Dict[str, UCDMConvertedField]]:
        if len(result) == 0:
            return []
        output: List[Dict[str, UCDMConvertedField]] = []
        serials: Dict[str, SerialGenerator] = {}

        for row in result:
            converted = self.convert_row(mapping_index, row, serials, str_to_int, fields_map, automation_strategies_map)
            for converted_row in converted:
                output.append(converted_row)

        return output

    def get_field_alias(self, origin: str) -> str:
        if not origin.startswith("c."):
            return "c." + origin
        return origin

    def get_field_table_alias(self, origin: str) -> str:
        if not origin.__contains__("."):
            return ""
        return origin.split('.')[0]

    def get_is_concept(
            self,
            automation_strategies_map: Dict[str, Dict[str, str]],
            bridge_id: str,
            field_alias: str
    ) -> bool:
        if not str(bridge_id) in automation_strategies_map:
            return False

        if not field_alias in automation_strategies_map[bridge_id]:
            return False

        if "valueMappingType" in automation_strategies_map[bridge_id][field_alias]:
            return automation_strategies_map[bridge_id][field_alias]["valueMappingType"] == "conceptId"

        return False

    def get_automation_strategy(
            self,
            automation_strategies_map: Dict[str, Dict[str, str]],
            bridge_id: str,
            field_alias: str
    ) -> str:
        if not bridge_id in automation_strategies_map:
            return ''

        if not field_alias in automation_strategies_map[bridge_id]:
            return ''

        if not "automationStrategy" in automation_strategies_map[bridge_id][field_alias]:
            return ''

        return automation_strategies_map[bridge_id][field_alias]["automationStrategy"]

    def convert_row(
            self,
            mapping_index: Dict[str, Dict[str, Dict[str, List[Tuple[str, str, str]]]]],
            row: Dict[str, str],
            serials: Dict[str, SerialGenerator],
            str_to_int: StrToIntGenerator,
            fields_map: Dict[str, Dict[str, str]],
            automation_strategies_map: Dict[str, Dict[str, str]]
    ) -> List[Dict[str, UCDMConvertedField]]:
        input_matrix: Dict[str, List[any]] = {}
        for field, value in row.items():
            field_alias = self.get_field_alias(field)
            table_alias = self.get_field_table_alias(field)

            if table_alias + '.__bridge_id' in row:
                bridge_id = row[table_alias + '.__bridge_id']
            else:
                bridge_id = None
            automation_strategy = self.get_automation_strategy(automation_strategies_map, bridge_id, field_alias)

            if not field_alias in mapping_index or not bridge_id in mapping_index[field_alias] or not str(value) in mapping_index[field_alias][bridge_id]:
                if field_alias in fields_map:
                    name_origin: str = fields_map[field_alias]['name']
                    is_required = fields_map[field_alias]['isRequired']
                    is_concept = self.get_is_concept(
                        automation_strategies_map,
                        bridge_id,
                        field_alias
                    )
                    if is_concept:
                        if is_required:
                            logging.warning("Value '{value}' is unmapped in the field '{name_origin}', bridge_id={bridge_id}. Skip row, field is required".format(
                                name_origin=name_origin, field=field, value=value, bridge_id=bridge_id))
                            return []
                        else:
                            logging.warning(
                                "Value '{value}' is unmapped in the field '{name_origin}', bridge_id={bridge_id}. Use blank field, field is optional".format(
                                    name_origin=name_origin, field=field, value=value, bridge_id=bridge_id))
                            values = [('', automation_strategy)]
                    else:
                        values = [(value, automation_strategy)]
                else:
                    values = [(value, automation_strategy)]
            else:
                values = mapping_index[field_alias][bridge_id][str(value)]

                if values[0][2] == 'Yes':
                    values = [(values[0][0], automation_strategy)]
                else:
                    values = [(value, automation_strategy)]
            input_matrix[field] = values

        multiplied = decartes_multiply_array(input_matrix)

        result: List[Dict[str, UCDMConvertedField]] = []
        for row_converted in multiplied:
            result_row: Dict[str, UCDMConvertedField] = {}
            for field, val in row_converted.items():
                export_value: str = val[0] if val[0] is not None else ''
                mapping_strategy = val[1]

                if mapping_strategy == 'convertStringToInt':
                    export_value = str(str_to_int.get_int(export_value))

                if mapping_strategy == 'serial':
                    if not field in serials:
                        serials[field] = SerialGenerator()
                    export_value = str(serials[field].get_next_value())
                result_row[field] = UCDMConvertedField(export_value)
            result.append(result_row)

        return result

def decartes_multiply_array(array: Dict[str, List[any]]) -> List[Dict[str, str]]:
    if not array:
        return []

    result = []
    for field_name, values in array.items():
        if not result:
            result = [{field_name: val} for val in values]
            continue

        result2 = []
        for val in values:
            for row in result:
                new_row = row.copy()
                new_row[field_name] = val
                result2.append(new_row)
        result = result2

    return result