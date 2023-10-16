from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message.StartMLTrain import StartMLTrain
from src.UCDM.Schema.BaseSchema import BaseSchema
from src.auto.auto_api_client.api.agent_api import AgentApi
import json
import logging
from sqlalchemy.exc import ProgrammingError
from typing import List, Dict
import csv
from typing import Optional


from src.auto.auto_api_client.api_client import ApiClient
from src.auto.auto_api_client.model.mapping_resolve_response import MappingResolveResponse


class MlTrain:
    api: Api
    adapter: BaseAdapter
    schema: BaseSchema
    def __init__(self, api: Api, adapter: BaseAdapter, schema: BaseSchema):
        self.api = api
        self.adapter = adapter
        self.schema = schema

    def start_model_train(self, message: StartMLTrain):
        sql = self.schema.resolve_cohort_definition_sql_query(message.cohort_definition['where'], message.cohort_definition['export'])
        logging.info("Model train task got: {}".format(json.dumps(sql)))

        try:
            result = self.schema.resolve_cohort_definition(sql)
            result_without_count = [{k: str(v) for k, v in d.items() if k != 'count'} for d in result]

            logging.info("SQL result: {}".format(json.dumps(result_without_count)))
            mapping = self.resolve_mapping(result_without_count, message)
            mapping_index = self.build_index(mapping)

            self.write_csv("/tmp/result.csv", mapping_index, result_without_count)
        except ProgrammingError as e:
            logging.error("SQL query error: {}".format(e.orig))
    def resolve_mapping(self, result, message) -> List[MappingResolveResponse]:
        request: Dict[str, List[str]] = {}
        for row in result:
            for field, value in row.items():
                if not field in request:
                    request[field] = []
                request[field].append(value)
                request[field] = list(set(request[field]))

        logging.info("Mapping resolution request: {}".format(json.dumps(request)))

        return self.api.resolve_mapping(message.cohort_definition['key'], request)

    def build_index(self, res: List[MappingResolveResponse]) -> Dict[str, Dict[str, str]]:
        index: Dict[str, Dict[str, str]] = {}
        for row in res:
            if not row['var_name'] in index:
                index[row['var_name']] = {}
            index[row['var_name']][row['bio_bank_value']] = row['ucdm_value']
        logging.info("Index: {}".format(json.dumps(index)))
        return index

    def write_csv(self, filename: str, mapping_index: Dict[str, Dict[str, str]], result: List[Dict[str, str]]):
        with open(filename, 'w', newline='') as file:
            if len(result) == 0:
                return
            writer = csv.DictWriter(file, fieldnames=result[0].keys())
            writer.writeheader()  # Writes the keys as headers
            for row in result:
                converted = self.convert_row(mapping_index, row)
                if converted is not None:
                    writer.writerow(converted)
                else:
                    logging.info("Skip writing row={}, as it's not converted".format(json.dumps(row)))


    def convert_row(self, mapping_index: Dict[str, Dict[str, str]], row: Dict[str, str]) -> Optional[Dict[str, str]]:
        converted: Dict[str, str] = {}
        for field, value in row.items():
            if not str(value) in mapping_index[field]:
                logging.info("Cant find in mapping field={}, value={}".format(field, value))
                return None
            converted[field] = mapping_index[field][str(value)]

        return converted