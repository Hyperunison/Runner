from src.UCDM.Schema.BaseSchema import BaseSchema
from src.UCDM.Schema.Omop2 import Omop2
from src.UCDM.Schema.Disabled import Disabled
from src.UCDM.Schema.Omop import Omop


def create_schema_by_config(config) -> BaseSchema:
    if config['schema'] == 'Omop2':
        return Omop2(config['dsn'], config['min_count'])
    if config['schema'] == 'Omop':
        return Omop(config['dsn'], config['min_count'])
    if config['schema'] == 'disabled':
        return Disabled()


class Factory:
    pass
