from src.UPDM.Schema.BaseSchema import BaseSchema
from src.UPDM.Schema.Omop2 import Omop2
from src.UPDM.Schema.Disabled import Disabled
from src.UPDM.Schema.Omop import Omop


def create_schema_by_config(config) -> BaseSchema:
    if config['schema'] == 'Omop2':
        return Omop2(config['dsn'], config['min_count'])
    if config['schema'] == 'Omop':
        return Omop(config['dsn'], config['min_count'])
    if config['schema'] == 'disabled':
        return Disabled()


class Factory:
    pass
