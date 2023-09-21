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