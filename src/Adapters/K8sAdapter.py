import logging
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


updateLabelPeriod = 30


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


class K8sAdapter(BaseAdapter):
    namespace: str = None
    image: str = None
    pod_prefix: str = None
    volumes: Dict[str, str] = None
    labels: Dict[str, str] = {}
    config: Dict[str, Dict[str, str]] = {}
    work_dir: str = None
    hostname: str = None
    api_client: Api = None
    observed_runs: {} = {}
    agent_id: int = None

    def __init__(self, api_client: Api, runner_instance_id: str, config, agent_id: int):
        self.namespace = config['executor']['namespace']
        self.image = config['executor']['image']
        self.pod_prefix = config['executor']['pod_prefix']
        self.volumes = config['executor']['volumes']
        self.labels = config['executor']['labels']
        self.api_client = api_client
        self.work_dir = config['executor']['work_dir']
        self.config = config
        self.hostname = runner_instance_id
        self.agent_id = agent_id

    def type(self):
        return 'k8s'

    def run_pipeline(self, nextflow_command: str, folder: str, run_id: int):
        k8s = K8sService(self.namespace)
        nextflow_command = 'cd ' + self.work_dir + "/" + folder + ';\n' + nextflow_command
        labels = self.get_labels(run_id)
        labels['folder'] = folder
        pod_name = k8s.create_pod(self.pod_prefix, nextflow_command, labels, self.image, self.volumes)
        k8s.wait_pod_status(pod_name, ['Running', 'Failed', 'Succeeded'], 60)
        self.process_send_pod_logs(k8s, pod_name, int(run_id))
        self.subscribe_pod_finished_and_upload_result_files(
            int(run_id), pod_name, k8s, folder, self.config['file_transfer']
        )

    def subscribe_pod_finished_and_upload_result_files(
            self, run_id: int, pod_name: str, k8s: K8sService, folder: str, config: Dict[str, str]
    ):
        pid = os.fork()
        # pid = 0

        if pid == 0:
            k8s.wait_pod_status(pod_name, ['Completed', 'Failed', 'Succeeded'], 86400 * 30, 30)
            file_transfer = create_file_transfer(config)
            file_transfer.init(run_id, self.agent_id)

            pipeline_config = yaml.safe_load(file_get_contents('var/{}/pipeline_config.yaml'.format(folder)))
            for mask in pipeline_config['output_file_masks'].keys():
                target = pipeline_config['output_file_masks'][mask]
                file_transfer.download(folder + '/' + mask, "var/" + folder + "/output/" + target)
            self.upload_local_file_to_s3(
                "var/" + folder + '/output', pipeline_config['aws_s3_path'], pipeline_config['aws_id'],
                pipeline_config['aws_key']
            )
            status = k8s.get_pod_status(pod_name)
            state = 'success' if (status == 'Succeeded' or status == 'Completed') else 'error'
            self.api_client.set_run_status(run_id, state)
            file_transfer.cleanup()
            sys.exit(0)

    def get_labels(self, run_id: int):
        labels = self.labels
        for label in labels.keys():
            labels[label] = labels[label].replace('{pod_prefix}', self.pod_prefix)
            labels[label] = labels[label].replace('{run_id}', str(run_id))
            labels[label] = labels[label].replace('{instance}', self.hostname)
            labels[label] = labels[label].replace('{last_connect}', str(int(float(time.time()))))
            labels[label] = labels[label].replace('{agent_id}', str(self.agent_id))

        return labels

    def process_get_process_logs(self, message: GetProcessLogs):
        k8s = K8sService(self.namespace)
        logs = k8s.get_pod_logs(message.process_id, message.lines_limit)
        self.api_client.set_process_logs(message.process_id, logs, message.reply_channel)

    def process_kill_job(self, message: KillJob) -> bool:
        k8s = K8sService(self.namespace)
        pods = k8s.search_pods({'type': self.pod_prefix, 'run_id': int(message.run_id)}, {})
        for pod in pods:
            k8s.delete_pod(pod['pod_name'])
        self.api_client.set_kill_result(message.run_id, message.channel)
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
        k8s = K8sService(self.namespace)
        pods = k8s.search_pods({'type': self.pod_prefix}, {})
        for pod in pods:
            pod_name = pod['metadata']['name']
            run_id = int(pod['metadata']['labels']['run_id'])
            agent_id = int(pod['metadata']['labels']['agent_id']) if 'agent_id' in pod['metadata']['labels'] else None
            status = pod['status']['phase']
            folder = str(pod['metadata']['labels']['folder'])
            last_connect = int(float((pod['metadata']['labels']['last_connect']))) if pod['metadata']['labels'][
                'last_connect'] else None
            instance = str(pod['metadata']['labels']['instance']) if pod['metadata']['labels']['instance'] else None

            logging.debug("{} - {} - {} - {} sec ago from {}".format(pod_name, run_id, status,
                                                                     int(float(time.time() - last_connect)), instance))
            if status == 'Succeeded' or status == 'Failed':
                if ((instance and instance == self.hostname)
                        or (agent_id == self.agent_id and int(
                            float(time.time())) - last_connect >= updateLabelPeriod * 6)):
                    state = 'success' if status == 'Succeeded' else 'error'

                    self.subscribe_pod_finished_and_upload_result_files(
                        int(run_id), pod_name, k8s, folder, self.config['file_transfer']
                    )

                    self.api_client.set_run_status(run_id, state)
                    k8s.delete_pod(pod_name)

                if run_id in self.observed_runs:
                    del self.observed_runs[run_id]
            elif status == 'Running':
                if instance and instance == self.hostname:
                    if run_id in self.observed_runs:
                        pid = self.observed_runs[run_id]
                        logging.info("pid {} - state {}".format(pid, _check_pid(pid)))
                        if not _check_pid(pid):
                            del self.observed_runs[run_id]
                        if not last_connect or time.time() - last_connect >= updateLabelPeriod:
                            k8s.add_label_to_pod(pod_name, "last_connect='{}'".format(int(float(time.time()))), True)
                    continue
                elif instance and last_connect and int(float(time.time())) - last_connect < updateLabelPeriod * 6:
                    continue

                if agent_id == self.agent_id and run_id not in self.observed_runs:
                    k8s.add_label_to_pod(pod_name, "instance={}".format(self.hostname), True)
                    k8s.add_label_to_pod(pod_name, "last_connect='{}'".format(int(float(time.time()))), True)
                    self.process_send_pod_logs(k8s, pod_name, int(run_id))
                    self.subscribe_pod_finished_and_upload_result_files(
                        int(run_id), pod_name, k8s, folder, self.config['file_transfer']
                    )


