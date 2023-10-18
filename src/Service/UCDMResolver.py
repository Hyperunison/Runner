from io import StringIO

from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message.StartMLTrain import StartMLTrain
from src.UCDM.Schema.BaseSchema import BaseSchema
import json
import logging
from sqlalchemy.exc import ProgrammingError
from typing import List, Dict
import csv
from typing import Optional
from src.auto.auto_api_client.model.mapping_resolve_response import MappingResolveResponse


class UCDMResolver:
    api: Api
    schema: BaseSchema
    def __init__(self, api: Api, schema: BaseSchema):
        self.api = api
        self.schema = schema

    def get_ucdm_result(self, cohort_definition) -> List[Dict[str, str]]:
        sql = self.schema.build_cohort_definition_sql_query(
            cohort_definition['where'],
            cohort_definition['export'],
            True
        )
        logging.info("Model train task got: {}".format(json.dumps(sql)))

        try:
            result = self.schema.resolve_cohort_definition(sql)
            result = self.normalize(result)
            result_without_count = [{k: v for k, v in d.items() if k != 'count'} for d in result]

            logging.info("SQL result: {}".format(json.dumps(result_without_count)))
            mapping = self.resolve_mapping(result_without_count, cohort_definition['key'])
            mapping_index = self.build_index(mapping)

            sql_final = self.schema.build_cohort_definition_sql_query(
                cohort_definition['where'],
                cohort_definition['export'],
                False
            )
            result = self.schema.resolve_cohort_definition(sql_final)
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

    def build_index(self, res: List[MappingResolveResponse]) -> Dict[str, Dict[str, str]]:
        index: Dict[str, Dict[str, str]] = {}
        for row in res:
            if not row['var_name'] in index:
                index[row['var_name']] = {}
            index[row['var_name']][row['bio_bank_value']] = row['ucdm_value']
        logging.info("Index: {}".format(json.dumps(index)))
        return index

    def convert_to_ucdm(self, result: List[Dict[str, str]], mapping_index: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
        if len(result) == 0:
            return []
        output:List[Dict[str, str]] = []
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


    def convert_row(self, mapping_index: Dict[str, Dict[str, str]], row: Dict[str, str]) -> Optional[Dict[str, str]]:
        converted: Dict[str, str] = {}
        for field, value in row.items():
            if not field in mapping_index:
                converted[field] = str(value)
            else:
                if not str(value) in mapping_index[field]:
                    logging.info("Cant find in mapping field={}, value={}".format(field, value))
                    return None
                converted[field] = mapping_index[field][str(value)]

        return converted