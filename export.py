import sys
import os

from src.Service.Csv.UCDMResultToCsvTransformer import UCDMResultToCsvTransformer
from src.Service.UCDMResolverTwo import UCDMResolver
from src.auto.auto_api_client.api import agent_api
from src.auto.auto_api_client.api_client import ApiClient

from src.UCDM.DataSchema import DataSchema
from src.Service.ConfigurationLoader import ConfigurationLoader
from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.Service.Csv.CsvToMappingTransformer import CsvToMappingTransformer
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator
from src.Api import Api

try:
    import pydevd_pycharm
    pydevd_pycharm.settrace('host.docker.internal', port=55147, stdoutToServer=True, stderrToServer=True)
except:
    pass

argv = sys.argv

if len(argv) != 3:
    print('Invalid arguments count!')
    print('Correct format is: python export.py query.sql mapping.csv > result.csv')
    exit(1)

if not os.path.isfile(argv[1]):
    print('File with source SQL does not exist!')
    exit(1)

if not os.path.isfile(argv[2]):
    print('File with mapping does not exist!')
    exit(1)

config = ConfigurationLoader("config.yaml").get_config()
allow_private_upload_data_to_unison = config['allow_private_upload_data_to_unison'] == 1
manager = ConsoleApplicationManager()
configuration = manager.initialize(config)

sql_query = open(argv[1], 'r').read()
mapping_abspath = os.path.abspath(argv[2])

t = CsvToMappingTransformer()
rows = t.transform_with_file_path(mapping_abspath)

schema = DataSchema(
    config['phenoenotypicDb']['dsn'],
    config['phenoenotypicDb']['schema'],
    config['phenoenotypicDb']['min_count']
)
str_to_int = StrToIntGenerator()

ucdm_mapping_resolver = UCDMMappingResolver(rows)

ucdm_resolver = UCDMResolver(schema, ucdm_mapping_resolver)
result = ucdm_resolver.get_ucdm_result(sql_query, str_to_int)

if result is None:
    print('Invalid UCDM result!')
    exit(1)

if len(result) < 1:
    print('Empty UCDM result!')
    exit(1)

csv_transformer = UCDMResultToCsvTransformer()
print(csv_transformer.transform(result))
