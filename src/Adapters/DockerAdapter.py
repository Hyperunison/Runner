import logging
import random
import shlex
import string
import subprocess
import time
import os
import sys
from typing import Dict
import yaml

from ..FileTransport.FileTransferFactory import create_file_transfer
from ..Service.K8s import K8s as K8sService
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs


def file_get_contents(file_path: str) -> str:
    with open(file_path, 'r') as file:
        return file.read()


def _check_pid(pid: int):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


class DockerAdapter(BaseAdapter):
    image: str = None
    container_prefix: str = None
    volumes: Dict[str, str] = None
    labels: Dict[str, str] = {}
    config: Dict[str, Dict[str, str]] = {}
    work_dir: str = None
    hostname: str = None
    api_client: Api = None
    observed_runs: {} = {}
    agent_id: int = None

    def __init__(self, api_client: Api, runner_instance_id: str, config, agent_id):
        self.image = config['executor']['image']
        self.container_prefix = config['executor']['container_prefix']
        self.volumes = config['executor']['volumes']
        self.labels = config['executor']['labels']
        self.api_client = api_client
        self.work_dir = config['executor']['work_dir'].replace('{home}', os.getcwd())
        self.volumes = config['executor']['volumes']
        self.config = config
        self.hostname = runner_instance_id
        self.agent_id = agent_id

    def type(self):
        return 'k8s'

    def run_pipeline(self, nextflow_command: str, folder: str, run_id: int):
        labels = self.get_labels(run_id)
        labels['folder'] = folder
        self.run_container(self.work_dir + "/" + folder, nextflow_command, labels)
        # self.process_send_pod_logs(k8s, pod_name, int(run_id))
        # self.subscribe_pod_finished_and_upload_result_files(
        #     int(run_id), pod_name, k8s, folder, self.config['file_transfer']
        # )

    def run_container(self, workdir: str, command, labels: Dict[str, str]) -> str:
        name = self.container_prefix + '-' + _random_word(16)
        cmd = "docker run -t -d --name {} -w {} ".format(name, workdir)
        for i in labels.keys():
            cmd += " -l {}={}".format(i, labels[i])
        for i in self.volumes.keys():
            cmd += " -v {}:{}".format(i, self.volumes[i])
        cmd += ' ' + self.image
        cmd += ' bash -c "{}"'.format(command)

        p = subprocess.run(shlex.split(cmd), capture_output=True)

        if p.returncode > 0:
            raise Exception("Can't start container, cmd={}".format(cmd))

        return name

    def subscribe_pod_finished_and_upload_result_files(
            self, run_id: int, pod_name: str, k8s: K8sService, folder: str, config: Dict[str, str]
    ):
        pid = os.fork()
        # pid = 0

        if pid == 0:
            k8s.wait_pod_status(pod_name, ['Completed', 'Failed', 'Succeeded'], 86400 * 30, 30)
            file_transfer = create_file_transfer(config)
            file_transfer.init()

            pipeline_config = yaml.safe_load(file_get_contents('var/{}/pipeline_config.yaml'.format(folder)))
            for mask in pipeline_config['output_file_masks'].keys():
                target = pipeline_config['output_file_masks'][mask]
                file_transfer.download(file_transfer.base_dir + "/" + folder + '/' + mask,
                                       "var/" + folder + "/output/" + target)
            self.upload_local_file_to_s3(
                "var/" + folder + '/output', pipeline_config['aws_s3_path'], pipeline_config['aws_id'],
                pipeline_config['aws_key']
            )
            self.api_client.set_run_status(run_id, 'success')
            file_transfer.cleanup()
            sys.exit(0)

    def get_labels(self, run_id: int):
        labels = self.labels
        for label in labels.keys():
            labels[label] = labels[label].replace('{container_prefix}', self.container_prefix)
            labels[label] = labels[label].replace('{run_id}', str(run_id))
            labels[label] = labels[label].replace('{instance}', self.hostname)
            labels[label] = labels[label].replace('{last_connect}', str(int(float(time.time()))))
            labels[label] = labels[label].replace('{agent_id}', str(self.agent_id))

        return labels

    def process_get_process_logs(self, message: GetProcessLogs):
        # @todo: implement
        return

    def process_kill_job(self, message: KillJob) -> bool:
        # @todo: implement
        return True

    def process_send_pod_logs(self, k8s: K8sService, pod_name: string, run_id: int):
        def send_logs_chunk_via_api(message: str):
            logging.debug("Output line for runId={}: {}".format(run_id, message))
            self.api_client.add_log_chunk(run_id, message)

        pid = os.fork()
        # pid = 0
        if pid == 0:
            logging.info("Forked, run in fork, pid={}".format(os.getpid()))
            # configure_logs(self.config, "child={}".format(os.getpid()))
            k8s.subscribe_pod_logs(pod_name, send_logs_chunk_via_api)
            logging.info("Socket finished, Exiting child")
            sys.exit(0)
        self.observed_runs[run_id] = pid

    def check_runs_statuses(self):
        # @todo: implement
        return

def _random_word(length: int):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))