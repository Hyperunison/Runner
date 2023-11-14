from src.UCDM.Schema.BaseSchema import BaseSchema
from src.UCDM.Schema.CBioPortal import CBioPortal
from src.UCDM.Schema.Ukbb import Ukbb
from src.UCDM.Schema.Disabled import Disabled
from src.UCDM.Schema.Universal import Universal


def create_schema_by_config(config) -> BaseSchema:
    if config['schema'] == 'Ukbb':
        return Ukbb(config['dsn'], config['min_count'])
    if config['schema'] == 'Universal':
        return Universal(config['dsn'], config['min_count'])
    if config['schema'] == 'CBioPortal':
        return CBioPortal(config['dsn'], config['min_count'], config['table'])
    if config['schema'] == 'disabled':
        return Disabled()


class Factory:
    pass
