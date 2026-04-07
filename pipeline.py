import argparse
import logging
import os
import socket
import string
import random
import sys
import yaml

from src.Adapters.AdapterFactory import create_by_config
from src.LogConfigurator import configure_logs
from src.Service.Workflows.External.ExternalPipeline import ExternalPipeline

class MockApiClient:
    def __init__(self):
        self.api_instance = MockApiInstance()

    def set_run_status(self, run_id, status):
        msg = "[pipeline] run_id={} → status={}".format(run_id, status)
        if status == 'success':
            logging.info(msg)
            print(msg, flush=True)
        elif status == 'error':
            logging.error(msg)
            print(msg, file=sys.stderr, flush=True)
        else:
            logging.info(msg)

    def set_run_dir(self, run_id, dirname):
        logging.info("[pipeline] run_id={} → dir={}".format(run_id, dirname))

    def add_log_chunk(self, run_id: str, message: str):
        logging.info(message)
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
    parser.add_argument('--ucdm', required=True, help='Input UCDM file')
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help=(
            'Increase log verbosity. '
            'Use -v for INFO, -vv for DEBUG, -vvv/-vvvv to also enable HTTP client debug logs.'
        ),
    )
    args = parser.parse_known_args()

    return args[0]


def apply_verbosity(verbose_count: int):
    """Override log level from config based on -v/-vv/-vvv/-vvvv flags."""
    if verbose_count == 0:
        return  # keep level from config.yaml

    level_map = {
        1: logging.INFO,    # -v
        2: logging.DEBUG,   # -vv
        3: logging.DEBUG,   # -vvv
        4: logging.DEBUG,   # -vvvv
    }
    level = level_map.get(verbose_count, logging.DEBUG)

    logging.getLogger().setLevel(level)
    logging.info("Log level overridden to {} via -{}".format(
        logging.getLevelName(level), 'v' * verbose_count
    ))

    if verbose_count >= 3:
        # Also enable HTTP/API client debug output
        logging.getLogger("auto_api_client.rest").setLevel(level)
        logging.getLogger("http.client").setLevel(level)
        logging.debug("HTTP client debug logging enabled")


def get_random_string(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


args = parse_cli_args()
pipeline = args.pipeline
params_yaml_filename = args.params
params_ucdm_filename = args.ucdm

with open("config.yaml", 'r') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
configure_logs(config, "main")
apply_verbosity(args.verbose)

runner_instance_id = socket.gethostname() + "-" + str(os.getpid())

pipeline_executor = create_by_config(MockApiClient(), config, runner_instance_id, 1)

pipeline = ExternalPipeline(MockApiClient(), pipeline_executor, None, pipeline)

with open(params_yaml_filename, 'r') as file:
    parameters = yaml.load(file, Loader=yaml.FullLoader)

with open(params_ucdm_filename, 'r') as file:
    ucdm = yaml.load(file, Loader=yaml.FullLoader)

run_id = 1
run_name = 'test_run'
dirname = 'test_pipeline_'+get_random_string(12)
s3_path = ''   # no S3 in test mode
aws_id = ''
aws_key = ''
pipeline.execute_with_ucdm(ucdm, parameters, run_id, run_name, '', dirname, s3_path, aws_id, aws_key)

# For docker/k8s adapters, run_pipeline forks child processes (log streamer + result collector).
# Wait for all children so pipeline.py doesn't exit before they report success/error.
logging.debug("Waiting for child processes to finish...")
while True:
    try:
        pid, raw_status = os.wait()
        exit_code = os.WEXITSTATUS(raw_status)
        logging.debug("Child pid={} finished, exit_code={}".format(pid, exit_code))
    except ChildProcessError:
        break  # no more children

logging.info("[pipeline] Done.")
