from typing import List, Dict, Tuple
import logging
from sqlalchemy.exc import ProgrammingError

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
    ) -> List[Dict[str, UCDMConvertedField]]:
        try:
            mapping_index = self.build_mapping_index()
            result = self.schema.fetch_all(sql_final)
            result = self.normalize(result)
            ucdm_result = self.convert_to_ucdm(result, mapping_index, str_to_int, fields_map, automation_strategies_map)

            return ucdm_result
        except ProgrammingError as e:
            logging.error("SQL query error: {}".format(e.orig))

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
            bridge_id = row['c.__bridge_id']
            if bridge_id in automation_strategies_map and field_alias in automation_strategies_map[bridge_id]:
                automation_strategy = automation_strategies_map[bridge_id][field_alias]
            else:
                automation_strategy = ''

            if not field_alias in mapping_index or not bridge_id in mapping_index[field_alias] or not str(value) in mapping_index[field_alias][bridge_id]:
                if field_alias in fields_map:
                    name_origin: str = fields_map[field_alias]['name']
                    is_required = fields_map[field_alias]['isRequired']
                    is_concept = fields_map[field_alias]['isConcept']
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