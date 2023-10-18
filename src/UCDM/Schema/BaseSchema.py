import json
import logging

from src.Api import Api
from src.Message.CohortAPIRequest import CohortAPIRequest


class BaseSchema:
    def __init__(self):
        pass
    def execute_cohort_definition(self, message: CohortAPIRequest, api: Api):
        where = message.cohort_definition['where']
        logging.info('Where got: %s'.format(json.dumps(where)))
        pass

    def resolve_cohort_definition(self, sql: str):
        logging.info('SQL query got: %s'.format(json.dumps(sql)))
        pass

    def build_cohort_definition_sql_query(self, where, export, distribution: bool) -> str:
        logging.info('Where got: %s, export got: %s'.format(json.dumps(where), json.dumps(export)))
        return ''
