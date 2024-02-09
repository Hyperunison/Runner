from io import StringIO

from src.Api import Api
import json
import logging
from sqlalchemy.exc import ProgrammingError
from typing import List, Dict, Tuple
import csv
from typing import Optional

from src.UCDM.DataSchema import DataSchema, VariableMapper
from src.auto.auto_api_client.model.mapping_resolve_response import MappingResolveResponse


class UCDMConvertedField:
    biobank_value: str
    ucdm_value: str
    omop_id: Optional[int]

    def __init__(self, biobank_value: str, ucdm_value: str, omop_id: Optional[int]):
        self.biobank_value = biobank_value
        self.ucdm_value = ucdm_value
        self.omop_id = omop_id


class UCDMResolver:
    api: Api
    schema: DataSchema

    def __init__(self, api: Api, schema: DataSchema):
        self.api = api
        self.schema = schema

    def get_ucdm_result(self, cohort_definition) -> List[Dict[str, str]]:
        mapper = VariableMapper(cohort_definition['fields'])

        sql = self.schema.build_cohort_definition_sql_query(
            mapper,
            cohort_definition['participantTableName'],
            cohort_definition['participantIdField'],
            cohort_definition['join'],
            cohort_definition['where'],
            cohort_definition['export'],
            1000000000,
            True
        )
        logging.info("Model train task got: {}".format(json.dumps(sql)))

        try:
            result = self.schema.fetch_all(sql)
            result = self.normalize(result)
            result_without_count = [{k: v for k, v in d.items() if k != 'count_uniq_participants'} for d in result]

            logging.info("SQL result: {}".format(json.dumps(result_without_count)))
            mapping = self.resolve_mapping(result_without_count, cohort_definition['key'])
            mapping_index = self.build_index(mapping)

            sql_final = self.schema.build_cohort_definition_sql_query(
                mapper,
                cohort_definition['participantTableName'],
                cohort_definition['participantIdField'],
                cohort_definition['join'],
                cohort_definition['where'],
                cohort_definition['export'],
                100,
                False
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
                    value = str(v)
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

        logging.info("Mapping resolution request: {}".format(json.dumps(request)))

        return self.api.resolve_mapping(key, request)

    def build_index(self, res: List[MappingResolveResponse]) -> Dict[str, Dict[str, Tuple[str, Optional[int]]]]:
        index: Dict[str, Dict[str, Tuple[str, Optional[int]]]] = {}
        for row in res:
            if not row['var_name'] in index:
                index[row['var_name']] = {}
            index[row['var_name']][row['bio_bank_value']] = (row['ucdm_value'], row['omop_id'])
        logging.info("Index: {}".format(json.dumps(index)))
        return index

    def convert_to_ucdm(self, result: List[Dict[str, str]], mapping_index: Dict[str, Dict[str, Tuple[str, Optional[int]]]]) -> List[Dict[str, UCDMConvertedField]]:
        if len(result) == 0:
            return []
        output: List[Dict[str, UCDMConvertedField]] = []
        for row in result:
            converted = self.convert_row(mapping_index, row)
            if converted is not None:
                output.append(converted)
            else:
                logging.info("Skip writing row={}, as it's not converted".format(json.dumps(row)))

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

    def convert_row(self, mapping_index: Dict[str, Dict[str, Tuple[str, Optional[int]]]], row: Dict[str, str]) -> Optional[Dict[str, UCDMConvertedField]]:
        converted: Dict[str, UCDMConvertedField] = {}
        for field, value in row.items():
            if not field in mapping_index:
                converted[field] = UCDMConvertedField(str(value), str(value), None)
            else:
                if not str(value) in mapping_index[field]:
                    logging.info("Cant find in mapping field={}, value={}".format(field, value))
                    return None
                ucdm_value: str = mapping_index[field][str(value)][0]
                omop_id: int = int(mapping_index[field][str(value)][1])
                converted[field] = UCDMConvertedField(str(value), str(ucdm_value), omop_id)

        return converted
