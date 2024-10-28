import sys

from src.Adapters import BaseAdapter
from src.Adapters.K8s import K8s
from src.Adapters.Slurm import Slurm
import logging


def create_by_config(api_client, config, runner_instance_id) -> BaseAdapter:
    if config['pipeline']['executor']['type'] == 'k8s':
        return K8s(api_client, runner_instance_id, config['pipeline'])
    else:
        logging.critical("Unknown adapter type " + config['pipeline']['executor']['type'])
        sys.exit(1)
