import sys
import os
import json
import string
import random

from src.Service.UCDMResolverTwo import UCDMResolver
from src.Service.Workflows.OMOPification.CsvWritter import CsvWritter

from src.UCDM.DataSchema import DataSchema
from src.Service.ConfigurationLoader import ConfigurationLoader
from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.Service.Csv.CsvToMappingTransformer import CsvToMappingTransformer
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator

def get_table_name(file_path: str) -> str:
    file_name = os.path.basename(file_path)
    parts = file_name.split('.')

    return parts[0]

def get_random_string(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

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

table_name = get_table_name(argv[1])
field_map_path = "var/" + table_name + "-fields-map.json"

if not os.path.isfile(field_map_path):
    print('Field map file does not exist!')
    exit(1)

with open(field_map_path) as json_data:
    fields_map = json.load(json_data)

config = ConfigurationLoader("config.yaml").get_config()
allow_private_upload_data_to_unison = config['allow_private_upload_data_to_unison'] == 1
manager = ConsoleApplicationManager()
configuration = manager.initialize(config)

sql_query = open(argv[1], 'r').read()
mapping_abspath = os.path.abspath(argv[2])

t = CsvToMappingTransformer()
rows = t.transform_with_file_path(
    mapping_abspath,
    table_name
)

schema = DataSchema(
    config['phenoenotypicDb']['dsn'],
    config['phenoenotypicDb']['schema'],
    config['phenoenotypicDb']['min_count']
)
str_to_int = StrToIntGenerator()
str_to_int.load_from_file()

ucdm_mapping_resolver = UCDMMappingResolver(rows)
ucdm_resolver = UCDMResolver(schema, ucdm_mapping_resolver)
result = ucdm_resolver.get_ucdm_result(sql_query, str_to_int)

if result is None:
    print('Invalid UCDM result!')
    exit(1)

if len(result) < 1:
    print('Empty UCDM result!')
    exit(1)

temp_csv_path = "var/" + get_random_string() + ".csv"
csv_writter = CsvWritter()
csv_writter.build(
    temp_csv_path,
    result,
    fields_map
)

with open(temp_csv_path) as file:
    print(file.read())

str_to_int.save_to_file()
os.remove(temp_csv_path)
