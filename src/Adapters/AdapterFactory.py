import sys

from src.Adapters import BaseAdapter
from src.Adapters.K8s import K8s
from src.Adapters.Slurm import Slurm
import logging


def create_by_config(api_client, config, runner_instance_id) -> BaseAdapter:
    if config['type'] == 'k8s':
        return K8s(api_client, config['work_dir'], runner_instance_id, config['k8s'], config)
    elif config['type'] == 'slurm':
        return Slurm(api_client, config['slurm'])
    else:
        logging.critical("Unknown adapter type " + config['type'])
        sys.exit(1)
