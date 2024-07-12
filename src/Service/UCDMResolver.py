from io import StringIO

from src.Api import Api
import json
import logging
from sqlalchemy.exc import ProgrammingError
from typing import List, Dict, Tuple
import csv
from typing import Optional

from src.Service.Workflows.SerialGenerator import SerialGenerator
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator
from src.UCDM.DataSchema import DataSchema, VariableMapper
from src.auto.auto_api_client.model.mapping_resolve_response import MappingResolveResponse


class UCDMConvertedField:
    export_value: str

    def __init__(self, export_value: str):
        self.export_value = export_value


class UCDMResolver:
    api: Api
    schema: DataSchema

    def __init__(self, api: Api, schema: DataSchema):
        self.api = api
        self.schema = schema

    def get_ucdm_result(self, cohort_definition) -> List[Dict[str, UCDMConvertedField]]:
        mapper = VariableMapper(cohort_definition['fields'])

        sql_with_distribution = self.schema.build_cohort_definition_sql_query(
            mapper,
            cohort_definition['participantTableName'],
            cohort_definition['participantIdField'],
            cohort_definition['join'],
            cohort_definition['where'],
            cohort_definition['export'],
            cohort_definition['cte'],
            None,
            cohort_definition['withTables'],
            True,
            False,
        )

        # logging.info("Model train task got: {}".format(json.dumps(sql)))

        try:
            result = self.schema.fetch_all(sql_with_distribution)
            result = self.normalize(result)
            result_without_count = [{k: v for k, v in d.items() if k != 'count_uniq_participants'} for d in result]

            # logging.info("SQL result: {}".format(json.dumps(result_without_count)))
            mapping = self.resolve_mapping(result_without_count, cohort_definition['key'])
            mapping_index = self.build_index(mapping)

            sql_final = self.schema.build_cohort_definition_sql_query(
                mapper,
                cohort_definition['participantTableName'],
                cohort_definition['participantIdField'],
                cohort_definition['join'],
                cohort_definition['where'],
                cohort_definition['export'],
                cohort_definition['cte'],
                cohort_definition['limit'],
                cohort_definition['withTables'],
                False,
                False,
            )
            result = self.schema.fetch_all(sql_final)
            result = self.normalize(result)

            ucdm_result = self.convert_to_ucdm(result, mapping_index)

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

    def resolve_mapping(self, result, key: str) -> List[MappingResolveResponse]:
        request: Dict[str, List[str]] = {}
        for row in result:
            for field, value in row.items():
                if not field in request:
                    request[field] = []
                request[field].append(value)
                request[field] = list(set(request[field]))

        # logging.info("Mapping resolution request: {}".format(json.dumps(request)))

        return self.api.resolve_mapping(key, request)

    def build_index(self, res: List[MappingResolveResponse]) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
        index: Dict[str, Dict[str, List[Tuple[str, str]]]] = {}
        for row in res:
            if not row['var_name'] in index:
                index[row['var_name']] = {}
            if not row['biobank_value'] in index[row['var_name']]:
                index[row['var_name']][row['biobank_value']] = []
            index[row['var_name']][row['biobank_value']].append((row['export_value'], row['automation_strategy']))
        # logging.info("Index: {}".format(json.dumps(index)))

        # deduplicate
        for key, val in index.items():
            for key2, val2 in val.items():
                index[key][key2] = list(set(val2))

        return index

    def convert_to_ucdm(self, result: List[Dict[str, str]], mapping_index: Dict[str, Dict[str, List[Tuple[str, str]]]]) -> List[Dict[str, UCDMConvertedField]]:
        if len(result) == 0:
            return []
        output: List[Dict[str, UCDMConvertedField]] = []
        serials: Dict[str, SerialGenerator] = {}
        str_to_int: Dict[str, StrToIntGenerator] = {}
        for row in result:
            converted = self.convert_row(mapping_index, row, serials, str_to_int)
            for converted_row in converted:
                output.append(converted_row)

        return output

    def get_csv_content(self, ucdm_result: List[Dict[str, str]]) -> str:
        output = StringIO()

        if len(ucdm_result) == 0:
            return ''
        writer = csv.DictWriter(output, fieldnames=ucdm_result[0].keys())
        writer.writeheader()  # Writes the keys as headers
        for row in ucdm_result:
            writer.writerow(row)

        return output.getvalue()

    def write_csv(self, filename: str, ucdm_result: List[Dict[str, str]]):
        with open(filename, 'w', newline='') as file:
            if len(ucdm_result) == 0:
                return
            writer = csv.DictWriter(file, fieldnames=ucdm_result[0].keys())
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm_result:
                writer.writerow(row)

    def convert_row2(
            self,
            mapping_index: Dict[str, Dict[str, Tuple[str, str]]],
            row: Dict[str, str],
            serials: Dict[str, SerialGenerator],
            str_to_int: Dict[str, StrToIntGenerator],
    ) -> Optional[Dict[str, UCDMConvertedField]]:
        converted: Dict[str, UCDMConvertedField] = {}
        for field, value in row.items():
            if not field in mapping_index:
                result = str(value)
            else:
                if not str(value) in mapping_index[field]:
                    # logging.info("Cant find in mapping field={}, value={}".format(field, value))
                    return None
                val = mapping_index[field][str(value)][0]
                export_value: str = val if val is not None else ''
                mapping_strategy = mapping_index[field][str(value)][1]

                if mapping_strategy == 'convertStringToInt':
                    if not field in str_to_int:
                        str_to_int[field] = StrToIntGenerator()
                    export_value = str(str_to_int[field].get_int(export_value))

                if mapping_strategy == 'serial':
                    if not field in serials:
                        serials[field] = SerialGenerator()
                    export_value = str(serials[field].get_next_value())
                result = export_value
            converted[field] = UCDMConvertedField(result)

        return converted

    def convert_row(
            self,
            mapping_index: Dict[str, Dict[str, List[Tuple[str, str]]]],
            row: Dict[str, str],
            serials: Dict[str, SerialGenerator],
            str_to_int: Dict[str, StrToIntGenerator],
    ) -> List[Dict[str, UCDMConvertedField]]:
        input_matrix: Dict[str, List[any]] = {}
        for field, value in row.items():
            if not field in mapping_index or not str(value) in mapping_index[field]:
                values = [(value, '')]
            else:
                values = mapping_index[field][str(value)]
            input_matrix[field] = values

        multiplied = decartes_multiply_array(input_matrix)

        result: List[Dict[str, UCDMConvertedField]] = []
        for row_converted in multiplied:
            result_row: Dict[str, UCDMConvertedField] = {}
            for field, val in row_converted.items():
                export_value: str = val[0] if val[0] is not None else ''
                mapping_strategy = val[1]

                if mapping_strategy == 'convertStringToInt':
                    if not field in str_to_int:
                        str_to_int[field] = StrToIntGenerator()
                    export_value = str(str_to_int[field].get_int(export_value))

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