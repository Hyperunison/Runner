from typing import List, Dict, Tuple
import logging
from sqlalchemy.exc import ProgrammingError

from src.Api import Api
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.Workflows.SerialGenerator import SerialGenerator
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator
from src.UCDM.DataSchema import DataSchema

class UCDMResolverSimple:
    api: Api
    schema: DataSchema
    ucdm_mapping_resolver: UCDMMappingResolver

    def __init__(self, api: Api, schema: DataSchema, ucdm_mapping_resolver: UCDMMappingResolver):
        self.api = api
        self.schema = schema
        self.ucdm_mapping_resolver = ucdm_mapping_resolver

    def get_ucdm_result(
            self,
            sql: str,
            str_to_int: StrToIntGenerator
    ) -> List[Dict[str, UCDMConvertedField]]:
        try:
            result = self.schema.fetch_all(sql)
            result = self.normalize(result)
            mapping_index = self.ucdm_mapping_resolver.transform_mapping_to_resolve_result()

            return self.convert_to_ucdm(
                result,
                mapping_index,
                str_to_int
            )
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

    def convert_to_ucdm(
            self,
            result: List[Dict[str, str]],
            mapping_index: Dict[str, Dict[str, List[Tuple[str, str]]]],
            str_to_int: StrToIntGenerator
    ) -> List[Dict[str, UCDMConvertedField]]:
        if len(result) == 0:
            return []
        output: List[Dict[str, UCDMConvertedField]] = []
        serials: Dict[str, SerialGenerator] = {}

        for row in result:
            converted = self.convert_row(mapping_index, row, serials, str_to_int)
            for converted_row in converted:
                output.append(converted_row)

        return output

    def convert_row(
            self,
            mapping_index: Dict[str, Dict[str, List[Tuple[str, str]]]],
            row: Dict[str, str],
            serials: Dict[str, SerialGenerator],
            str_to_int: StrToIntGenerator,
    ) -> List[Dict[str, UCDMConvertedField]]:
        input_matrix: Dict[str, List[any]] = {}
        for field, value in row.items():
            if not field in mapping_index or not str(value) in mapping_index[field]:
                values = [(value, '')]
            else:
                values = mapping_index[field][str(value)]
            input_matrix[field] = values

        multiplied = self.decartes_multiply_array(input_matrix)

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

    def decartes_multiply_array(self, array: Dict[str, List[any]]) -> List[Dict[str, str]]:
        if not array:
            return []

        result = []
        for key, values in array.items():
            if not result:
                result = [{key: val} for val in values]
                continue

            result2 = []
            for val in values:
                for row in result:
                    new_row = row.copy()
                    new_row[key] = val
                    result2.append(new_row)
            result = result2

        return result