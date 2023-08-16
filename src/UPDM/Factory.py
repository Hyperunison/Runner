from src.UPDM.Schema.BaseSchema import BaseSchema
from src.UPDM.Schema.CustomGenomicsEngland import CustomGenomicsEngland
from src.UPDM.Schema.Disabled import Disabled


def create_schema_by_config(config) -> BaseSchema:
    if config['schema'] == 'customGenomicsEngland':
        return CustomGenomicsEngland(config['dsn'])
    if config['schema'] == 'disabled':
        return Disabled()


class Factory:
    pass
