import json
import logging
import random
import shlex
import string
import subprocess
import tempfile
import time
import os
import sys
from typing import Dict, Optional

import yaml

from ..FileTransport.FileTransferFactory import create_file_transfer
from ..Service.K8s import K8s as K8sService
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun

sendLogsPeriod = 3
updateLabelPeriod = 30


def file_get_contents(file_path: str) -> str:
    with open(file_path, 'r') as file:
        return file.read()


def file_put_contents(file_path: str, content: str):
    with open(file_path, 'w') as file:
        file.write(content)


def create_temp_file(content: str) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, prefix="upload_tmp_file", suffix=".bin", mode='w')
    tmp.write(content)
    tmp.close()
    return tmp.name


class K8s(BaseAdapter):
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

    def __init__(self, api_client: Api, runner_instance_id: str, config):
        self.namespace = config['executor']['namespace']
        self.image = config['executor']['image']
        self.pod_prefix = config['executor']['pod_prefix']
        self.volumes = config['executor']['volumes']
        self.labels = config['executor']['labels']
        self.api_client = api_client
        self.work_dir = config['executor']['work_dir']
        self.config = config
        self.hostname = runner_instance_id
        self.agent_id = self.api_client.get_agent_id()

    def type(self):
        return 'k8s'

    def process_nextflow_run(self, message: NextflowRun, config: Dict) -> bool:
        input_files: Dict[str, str] = {
            'main.nf': message.nextflow_code,
            'data.json': json.dumps(message.input_data),
            'nextflow.config': file_get_contents('nextflow.config'),
        }

        output_file_masks: Dict[str, str] = {
            ".nextflow.log": "/basic/",
            "trace-*.txt": "/basic/",
        }

        return self.run_nextflow_run_abstract(
            message.run_id,
            message.command,
            message.dir,
            message.aws_s3_path,
            input_files,
            output_file_masks,
            message.aws_id,
            message.aws_key
        )

    def run_nextflow_run_abstract(
            self, run_id: int, nextflow_command: str, workdir: Optional[str], aws_s3_path: str,
            input_files: Dict[str, str], output_file_masks: Dict[str, str], aws_id: str, aws_key: str
    ):
        self.api_client.set_run_status(run_id, 'process')
        k8s = K8sService(self.namespace)
        file_transfer = create_file_transfer(self.config['file_transfer'])
        self.api_client.api_instance.api_client.close()
        self.api_client.api_instance.api_client.rest_client.pool_manager.clear()
        folder = self.get_pipeline_remote_dir_name(workdir)

        logging.info("Uploading workflow files")

        self.api_client.set_run_dir(run_id, folder)

        file_transfer.init()
        k8s.wait_pod_status(file_transfer.pod_name, ['Running'])

        file_transfer.mkdir(file_transfer.base_dir + "/" + folder)

        for file in input_files.keys():
            file_transfer.upload(create_temp_file(input_files[file]),
                                 file_transfer.base_dir + "/" + folder + "/" + file)

        file_transfer.cleanup()
        labels = self.get_labels(run_id)
        labels['folder'] = folder

        pipeline_config: Dict = {
            'aws_id': aws_id,
            'aws_key': aws_key,
            'aws_s3_path': aws_s3_path,
            'output_file_masks': output_file_masks,
        }
        os.mkdir("var/" + folder)
        file_put_contents('var/{}/pipeline_config.yaml'.format(folder), yaml.dump(pipeline_config))

        nextflow_command = 'cd ' + self.work_dir + "/" + folder + ';\n' + nextflow_command
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
            labels[label] = labels[label].replace('{pod_prefix}', self.pod_prefix)
            labels[label] = labels[label].replace('{run_id}', str(run_id))
            labels[label] = labels[label].replace('{instance}', self.hostname)
            labels[label] = labels[label].replace('{last_connect}', str(int(float(time.time()))))
            labels[label] = labels[label].replace('{agent_id}', str(self.agent_id))

        return labels

    def get_pipeline_remote_dir_name(self, work_dir: str) -> str:
        if work_dir == "" or work_dir is None:
            return 'pipeline_' + self._random_word(16)
        else:
            return work_dir.replace(self.work_dir, "")

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
                        logging.info("pid {} - state {}".format(pid, self._check_pid(pid)))
                        if not self._check_pid(pid):
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

    def upload_local_file_to_s3(self, filename: str, s3_path: str, aws_id: str, aws_key: str) -> bool:
        logging.info("Uploading local file {} to {}".format(filename, s3_path))

        cmd = (
            'bash -c \'export AWS_ACCESS_KEY_ID="{}"; export AWS_SECRET_ACCESS_KEY="{}"; aws s3 cp --recursive {} {}\''.
            format(aws_id, aws_key, filename, s3_path))

        logging.info("Executing command: {}".format(cmd))
        p = subprocess.run(shlex.split(cmd), capture_output=True)

        if p.returncode > 0:
            logging.critical(
                "Can't upload file to S3, stdout={}, error={}, return_code={}".format(cmd, p.stdout, p.stderr,
                                                                                      p.returncode))
            return False

        return True

    def _random_word(self, length: int):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))

    def _check_pid(self, pid: int):
        """ Check For the existence of a unix pid. """
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True
