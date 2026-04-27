import glob as glob_module
import json
import logging
import random
import re
import shlex
import shutil
import string
import subprocess
import time
import os
import sys
from typing import Dict, List, Optional
import yaml

from ..FileTransport.FileTransferFactory import create_file_transfer
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs


sendLogsPeriod = 3


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
        self.config = config
        self.hostname = runner_instance_id
        self.agent_id = agent_id

    def type(self):
        return 'docker'

    def run_pipeline(self, nextflow_command: str, folder: str, run_id: int):
        labels = self.get_labels(run_id)
        labels['folder'] = folder
        logging.info("Starting container for run_id={}, folder={}".format(run_id, folder))
        container_name = self.run_container(self.work_dir + "/" + folder, nextflow_command, labels)
        logging.info("Container started: name={}".format(container_name))
        self.process_send_container_logs(container_name, int(run_id))
        self.subscribe_container_finished_and_upload_result_files(int(run_id), container_name, folder)

    def run_container(self, workdir: str, command: str, labels: Dict[str, str]) -> str:
        name = self.container_prefix + '-' + _random_word(16)
        docker_gid = os.stat('/var/run/docker.sock').st_gid
        cmd = ['docker', 'run', '-t', '-d', '--name', name, '-w', workdir, '--group-add', str(docker_gid)]
        for k, v in labels.items():
            cmd += ['-l', '{}={}'.format(k, v)]
        for src, dst in self.volumes.items():
            cmd += ['-v', '{}:{}'.format(src, dst)]
        cmd.append(self.image)
        cmd += ['bash', '-c', command]

        logging.debug("Executing: {}".format(' '.join(cmd)))
        p = subprocess.run(cmd, capture_output=True)

        if p.returncode > 0:
            logging.error("Can't start container, stderr={}".format(p.stderr.decode()))
            raise Exception("Can't start container, cmd={}".format(cmd))

        return name

    def process_send_container_logs(self, container_name: str, run_id: int):
        def send_chunk(message: str):
            logging.debug("Log chunk for run_id={}: {}".format(run_id, message))
            self.api_client.add_log_chunk(run_id, message)

        pid = os.fork()
        if pid == 0:
            logging.info("Log streamer forked, pid={}, container={}".format(
                os.getpid(), container_name
            ))
            self._subscribe_container_logs(container_name, send_chunk)
            logging.info("Log stream ended for container={}".format(container_name))
            sys.exit(0)
        self.observed_runs[run_id] = pid

    def _subscribe_container_logs(self, container_name: str, callback):
        """Stream container stdout/stderr to callback, batching every sendLogsPeriod seconds."""
        cmd = ['docker', 'logs', '-f', container_name]
        logging.debug("Executing: {}".format(' '.join(cmd)))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        last_send = time.perf_counter()
        buffer = ''
        while line := p.stdout.readline().decode('utf-8', errors='replace'):
            clean = _strip_ansi(line)
            logging.debug("Container log: {}".format(clean.strip()))
            buffer += clean
            if time.perf_counter() - last_send > sendLogsPeriod and buffer:
                callback(buffer)
                buffer = ''
                last_send = time.perf_counter()

        if buffer:
            callback(buffer)

    def subscribe_container_finished_and_upload_result_files(
            self, run_id: int, container_name: str, folder: str
    ):
        pid = os.fork()
        if pid == 0:
            logging.info("Result collector forked, pid={}, container={}".format(
                os.getpid(), container_name
            ))
            exit_code = self._wait_container(container_name)

            pipeline_config = yaml.safe_load(
                file_get_contents('var/{}/pipeline_config.yaml'.format(folder))
            )

            output_dir = "var/{}/output".format(folder)
            os.makedirs(output_dir, exist_ok=True)

            file_transfer = create_file_transfer(self.config['file_transfer'])
            file_transfer.init(run_id, self.agent_id)

            logging.info("Downloading output files for folder={}".format(folder))
            for mask, target in pipeline_config['output_file_masks'].items():
                self._download_with_glob(file_transfer, folder, mask, output_dir, target)

            s3_path = pipeline_config.get('aws_s3_path', '')
            if s3_path and s3_path.startswith('s3://'):
                logging.info("Uploading to S3: {}".format(s3_path))
                self.upload_local_file_to_s3(
                    output_dir,
                    s3_path,
                    pipeline_config['aws_id'],
                    pipeline_config['aws_key']
                )
            elif s3_path:
                logging.debug("Skipping S3 upload: not a valid s3:// path ({})".format(s3_path))

            state = 'success' if exit_code == 0 else 'error'
            logging.info("Container {} finished, exit_code={}, state={}".format(
                container_name, exit_code, state
            ))
            self.api_client.set_run_status(run_id, state)
            file_transfer.cleanup()
            sys.exit(0)

    def _download_with_glob(self, file_transfer, folder: str, mask: str, output_dir: str, target: str):
        """Copy files matching glob mask into output_dir according to the target mapping.

        target can be:
          "/basic/"           — directory: copy each matched file into output_dir/basic/
          "/basic/result.txt" — file path: copy (first) matched file to output_dir/basic/result.txt
          "result.txt"        — file path: copy (first) matched file to output_dir/result.txt
        """
        base = file_transfer.base_dir.rstrip('/')
        pattern = os.path.join(base, folder, mask)
        matches = glob_module.glob(pattern)
        if not matches:
            logging.warning("No files matched pattern: {}".format(pattern))
            return

        target_is_dir = target.endswith('/')
        target_rel = target.strip('/')
        target_base = os.path.join(output_dir, target_rel) if target_rel else output_dir

        if target_is_dir:
            os.makedirs(target_base, exist_ok=True)

        for src_path in matches:
            if target_is_dir:
                dst_path = os.path.join(target_base, os.path.basename(src_path))
            else:
                dst_path = target_base
                os.makedirs(os.path.dirname(dst_path) or '.', exist_ok=True)
            logging.debug("Copying {} -> {}".format(src_path, dst_path))
            if os.path.isdir(src_path):
                shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
            else:
                shutil.copy(src_path, dst_path)

    def _wait_container(self, container_name: str) -> int:
        """Block until container exits, return its exit code."""
        logging.info("Waiting for container={} to finish".format(container_name))
        cmd = ['docker', 'wait', container_name]
        logging.debug("Executing: {}".format(' '.join(cmd)))
        p = subprocess.run(cmd, capture_output=True)
        if p.returncode > 0:
            logging.error("docker wait failed for {}: {}".format(
                container_name, p.stderr.decode()
            ))
            return 1
        try:
            exit_code = int(p.stdout.decode().strip())
        except ValueError:
            logging.error("Can't parse exit code from docker wait output: {!r}".format(
                p.stdout.decode()
            ))
            exit_code = 1
        logging.debug("Container {} exited with code {}".format(container_name, exit_code))
        return exit_code

    def _inspect_container(self, container_name: str) -> Optional[Dict]:
        """Return docker inspect JSON for a single container, or None on failure."""
        cmd = ['docker', 'inspect', container_name]
        p = subprocess.run(cmd, capture_output=True)
        if p.returncode > 0:
            logging.warning("Can't inspect container {}: {}".format(
                container_name, p.stderr.decode()
            ))
            return None
        try:
            data = json.loads(p.stdout.decode())
            return data[0] if data else None
        except (json.JSONDecodeError, IndexError) as e:
            logging.error("Can't parse docker inspect output for {}: {}".format(container_name, e))
            return None

    def _find_containers_by_labels(self, label_filters: Dict[str, str]) -> List[str]:
        """Return names of containers matching all given label key=value filters."""
        cmd = ['docker', 'ps', '-a', '--format', '{{.Names}}']
        for k, v in label_filters.items():
            cmd += ['--filter', 'label={}={}'.format(k, v)]
        logging.debug("Executing: {}".format(' '.join(cmd)))
        p = subprocess.run(cmd, capture_output=True)
        if p.returncode > 0:
            logging.error("Can't list containers: {}".format(p.stderr.decode()))
            return []
        output = p.stdout.decode().strip()
        return [name for name in output.split('\n') if name] if output else []

    def _stop_and_remove_container(self, container_name: str):
        logging.info("Stopping container: {}".format(container_name))
        subprocess.run(['docker', 'stop', '--time', '10', container_name], capture_output=True)
        logging.debug("Removing container: {}".format(container_name))
        subprocess.run(['docker', 'rm', container_name], capture_output=True)

    def process_get_process_logs(self, message: GetProcessLogs):
        container_name = message.process_id
        logging.info("Getting logs for container={}, lines={}".format(
            container_name, message.lines_limit
        ))
        cmd = ['docker', 'logs', '--tail', str(message.lines_limit), container_name]
        logging.debug("Executing: {}".format(' '.join(cmd)))
        p = subprocess.run(cmd, capture_output=True)
        if p.returncode == 0:
            logs = p.stdout.decode('utf-8', errors='replace')
        else:
            logs = "Can't get container logs"
            logging.error("Can't get logs for {}: {}".format(container_name, p.stderr.decode()))
        self.api_client.set_process_logs(message.process_id, logs, message.reply_channel)

    def process_kill_job(self, message: KillJob) -> bool:
        logging.info("Killing containers for run_id={}".format(message.run_id))
        containers = self._find_containers_by_labels({
            'type': self.container_prefix,
            'run_id': str(message.run_id),
        })
        if not containers:
            logging.warning("No containers found for run_id={}".format(message.run_id))
        for container_name in containers:
            self._stop_and_remove_container(container_name)
        self.api_client.set_kill_result(message.run_id, message.channel)
        return True

    def check_runs_statuses(self):
        containers = self._find_containers_by_labels({'type': self.container_prefix})
        for container_name in containers:
            info = self._inspect_container(container_name)
            if not info:
                continue

            labels = info.get('Config', {}).get('Labels', {}) or {}
            state_info = info.get('State', {})
            status = state_info.get('Status', '')  # running, exited, created, dead

            run_id_str = labels.get('run_id')
            folder = labels.get('folder')
            instance = labels.get('instance')
            agent_id_str = labels.get('agent_id')
            last_connect_str = labels.get('last_connect')

            if not run_id_str or not folder:
                continue

            run_id = int(run_id_str)
            agent_id = int(agent_id_str) if agent_id_str else None
            last_connect = int(float(last_connect_str)) if last_connect_str else None

            logging.debug("{} - run_id={} - status={}".format(container_name, run_id, status))

            if status in ('exited', 'dead'):
                if (instance and instance == self.hostname) or (
                    agent_id == self.agent_id
                    and last_connect
                    and int(time.time()) - last_connect >= 30 * 6
                ):
                    self.subscribe_container_finished_and_upload_result_files(
                        run_id, container_name, folder
                    )
                    self._stop_and_remove_container(container_name)

                if run_id in self.observed_runs:
                    del self.observed_runs[run_id]

            elif status == 'running':
                if instance and instance == self.hostname and run_id in self.observed_runs:
                    pid = self.observed_runs[run_id]
                    logging.info("pid={} - alive={}".format(pid, _check_pid(pid)))
                    if not _check_pid(pid):
                        del self.observed_runs[run_id]

    def get_labels(self, run_id: int) -> Dict[str, str]:
        labels = dict(self.labels)  # copy to avoid mutating class-level dict
        for label in labels.keys():
            labels[label] = labels[label].replace('{container_prefix}', self.container_prefix)
            labels[label] = labels[label].replace('{run_id}', str(run_id))
            labels[label] = labels[label].replace('{instance}', self.hostname)
            labels[label] = labels[label].replace('{last_connect}', str(int(float(time.time()))))
            labels[label] = labels[label].replace('{agent_id}', str(self.agent_id))
        return labels


def _random_word(length: int):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


_ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes (colors, cursor movement, etc.) from text."""
    return _ANSI_ESCAPE.sub('', text)
