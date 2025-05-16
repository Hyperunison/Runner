import argparse
import logging
import os
import socket
import string
import random
import yaml

from src.Adapters.AdapterFactory import create_by_config
from src.LogConfigurator import configure_logs
from src.Service.Workflows.External.ExternalPipeline import ExternalPipeline

class MockApiClient:
    def __init__(self):
        self.api_instance = MockApiInstance()
    def set_run_status(self, run_id, status):
        pass
    def set_run_dir(self, run_id, status):
        pass
    def add_log_chunk(self, run_id: str, message: str):
        logging.info(message)
        pass
class MockApiInstance:
    def __init__(self):
        self.api_client = MockApiClientInternal()

class MockApiClientInternal:
    def __init__(self):
        self.rest_client = MockRestClient()
        pass
    def close(self):
        pass
class MockRestClient:
    def __init__(self):
        self.pool_manager = MockPoolManager()

class MockPoolManager:
    def __init__(self):
        pass
    def clear(self):
        pass


def parse_cli_args():
    parser = argparse.ArgumentParser(
        prog='Unison pipeline tester',
        description='Script to run pipelines in test format without backend',
        epilog='')
    parser.add_argument('--pipeline', required=True, help='Path to the pipeline file (main.py)')
    parser.add_argument('--params', required=True, help='Path to the params file')
    args = parser.parse_known_args()

    return args[0]


def get_random_string(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


args = parse_cli_args()
pipeline = args.pipeline
params_yaml_filename = args.params

with open("config.yaml", 'r') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
configure_logs(config, "main")
runner_instance_id = socket.gethostname() + "-" + str(os.getpid())

pipeline_executor = create_by_config(MockApiClient(), config, runner_instance_id, 1)

pipeline = ExternalPipeline(MockApiClient(), pipeline_executor, None, pipeline)

ucdm = []
with open(params_yaml_filename, 'r') as file:
    parameters = yaml.load(file, Loader=yaml.FullLoader)
run_id = 1
run_name = 'test_run'
dirname = 'test_pipeline_'+get_random_string(12)
s3_path = 'test_run'
aws_id = ''
aws_key = ''
pipeline.execute_with_ucdm(ucdm, parameters, run_id, run_name, '', dirname, s3_path, aws_id, aws_key)




