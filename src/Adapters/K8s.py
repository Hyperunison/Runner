import json
import logging
import pipes
import random
import shlex
import string
import subprocess
import tempfile
import time
import hashlib
import os
import sys

from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun
from src.LogConfigurator import configure_logs


sendLogsPeriod = 3
updateLabelPeriod = 30


class K8s(BaseAdapter):
    namespace: str = None
    pod_prefix: str = None
    volume: str = None
    master_pod: str = None
    work_dir: str = None
    hostname: str = None
    api_client: Api = None
    observed_runs: {} = {}
    config: [] = None
    agent_id: int = None

    def __init__(self, api_client: Api, work_dir: str, runner_instance_id:str, config, full_config):
        self.namespace = config['namespace']
        self.pod_prefix = config['pod_prefix']
        self.volume = config['volume']
        self.master_pod = config['master_pod']
        self.api_client = api_client
        self.work_dir = work_dir
        self.config = full_config
        self.hostname = runner_instance_id
        self.agent_id = self.api_client.get_agent_id()

    def type(self):
        return 'k8s'

    def process_nextflow_run(self, message: NextflowRun) -> bool:
        # create folder
        # upload data.json and main.nf

        self.api_client.set_run_status(message.run_id, 'process')

        folder = message.dir
        if folder == "" or folder is None:
            folder = 'tmp_' + self._random_word(16)
        folder = folder.replace(self.work_dir, "")

        upload_log_cmd = self._get_upload_file_to_s3_cmd(self.work_dir+"/"+folder, ".nextflow.log", message.aws_s3_path+"/basic/")
        upload_trace_cmd = self._get_upload_file_to_s3_cmd(self.work_dir+"/"+folder, "trace-*.txt", message.aws_s3_path+"/basic/")
        cmd_text = '''
        mkdir -p {workdir} && cd {workdir}; 
        {nextflow_cmd}; 
        exit_code=$?;
        {upload_log_cmd}; 
        exit_code_upload_log=$?;
        {upload_trace_cmd};
        exit_code_upload_trace=$?;
        exit $exit_code && $exit_code_upload_log && $exit_code_upload_trace;
        '''.format(
            workdir = self.work_dir + '/' + folder,
            nextflow_cmd = message.command,
            upload_log_cmd = upload_log_cmd,
            upload_trace_cmd = upload_trace_cmd,
        )
        logging.info("RAW command: {}".format(cmd_text))
        cmd = self.get_kube_create_cmd(cmd_text, message.run_id)
        args = shlex.split(cmd)
        logging.info("Executing command: {}".format(cmd))
        p = subprocess.run(args, capture_output=True)

        if p.returncode > 0:
            logging.critical("Can't create pod, stdout={}, error={}".format(cmd, p.stdout, p.stderr))
            return False

        # todo: send folder name to server
        self._create_folder_remote(folder)

        # upload aws credentials
        self._upload_file(message.nextflow_code, folder + '/main.nf')
        self._upload_file(json.dumps(message.input_data), folder + '/data.json')
        self._upload_file("[default]\nregion = eu-central-1\n", folder+"/aws_config")
        self._upload_file(
            "[default]\naws_access_key_id={}\naws_secret_access_key={}\n".format(message.aws_id, message.aws_key),
            folder+"/aws_credentials"
        )
        # hack
        # cmd = 'bash -c "for i in {1..3}; do sleep 1; echo test; done"'
        self.process_send_pod_logs(self.master_pod, int(message.run_id))
        return True

    def process_get_process_logs(self, message: GetProcessLogs) -> bool:
        cmd = "kubectl --namespace={} logs --tail {} {}".format(self.namespace, int(message.lines_limit), message.process_id)
        logging.info("Executing command: {}".format(cmd))
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)

        if p.returncode == 0:
            logs = str(p.stdout.decode('utf-8'))
        else:
            logs = "Can't get logs"
            logging.error("Can't get logs, stdout={}, error={}".format(p.stdout.decode('utf-8'), p.stderr.decode('utf-8')))

        self.api_client.set_process_logs(message.process_id, logs, message.reply_channel)

        return True

    def process_kill_job(self, message: KillJob) -> bool:
        cmd = "kubectl --namespace={} get pods --selector=type={} --selector=run_id={} -o json".format(self.namespace, self.pod_prefix, int(message.run_id))
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0:
            logging.critical("Can't find pod, output={}, stderr: {}".format(p.stdout, p.stderr))
            return
        data = json.loads(str(p.stdout.decode('utf-8')))
        if data['items'] and data['items'][0]:
            pod_name = data['items'][0]['metadata']['name']
            self.delete_pod(pod_name)
        self.api_client.set_kill_result(message.run_id, message.channel)
        return True

    def process_send_pod_logs(self, pod_name: string, run_id: int):
        pid = os.fork()
        if pid == 0:
            logging.info("Forked, run in fork, pid={}".format(os.getpid()))
            configure_logs(self.config, "child={}".format(os.getpid()))

            cmd = "kubectl --namespace={} logs {} -c {} -f".format(self.namespace, pod_name, pod_name)
            args = shlex.split(cmd)
            logging.info("Executing command: {}".format(cmd))
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            last_send = time.perf_counter()
            buffer = ''
            while line := p.stdout.readline().decode("utf-8"):
                logging.info('Stdout line: {}'.format(line.strip()))
                buffer += line
                if time.perf_counter() - last_send > sendLogsPeriod and buffer != '':
                    self.api_client.add_log_chunk(run_id, buffer)
                    buffer = ''
                    last_send = time.perf_counter()

            logging.info("Socket finished")

            if buffer != '':
                self.api_client.add_log_chunk(run_id, buffer)

            logging.info("Exit code={}".format(p.returncode))
            logging.info("Exiting child")
            sys.exit(0)
        self.observed_runs[run_id] = pid

    def add_label_to_pod(self, pod_name: str, label: str, override = False):
        cmd = "kubectl --namespace={} label pods {} {} {}".format(self.namespace, pod_name, label, ('--overwrite' if override else ''))
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0:
            logging.critical("Can't add label to pod, output={}, stderr: {}".format(p.stdout, p.stderr))
            return False
        return True


    def check_runs_statuses(self):
        cmd = "kubectl --namespace={} get pods --selector='type={}' -o json".format(self.namespace, self.pod_prefix)
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0:
            logging.critical("Can't check pods statuses, output={}, stderr: {}".format(p.stdout, p.stderr))
            return
        data = json.loads(str(p.stdout.decode('utf-8')))
        for pod in data['items']:
            pod_name = pod['metadata']['name']
            run_id = int(pod['metadata']['labels']['run_id'])
            agent_id = int(pod['metadata']['labels']['agent_id']) if 'agent_id' in pod['metadata']['labels'] else None
            status = pod['status']['phase']
            last_connect = int(float((pod['metadata']['labels']['last_connect']))) if pod['metadata']['labels']['last_connect'] else None
            instance = str(pod['metadata']['labels']['instance']) if pod['metadata']['labels']['instance'] else None

            logging.debug("{} - {} - {} - {} sec ago from {}".format(pod_name, run_id, status, int(float(time.time() - last_connect)), instance))
            if status == 'Succeeded' or status == 'Failed':
                if ((instance and instance == self.hostname)
                        or (agent_id == self.agent_id and int(float(time.time())) - last_connect >= updateLabelPeriod * 6)):
                    state = 'success' if status == 'Succeeded' else 'error'
                    self.api_client.set_run_status(run_id, state)
                    self.delete_pod(pod_name)

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
                            self.add_label_to_pod(pod_name, "last_connect='{}'".format(int(float(time.time()))), True)
                    continue
                elif instance and last_connect and int(float(time.time())) - last_connect < updateLabelPeriod * 6:
                    continue

                if agent_id == self.agent_id and run_id not in self.observed_runs:
                    self.add_label_to_pod(pod_name, "instance={}".format(self.hostname), True)
                    self.add_label_to_pod(pod_name, "last_connect='{}'".format(int(float(time.time()))), True)
                    self.process_send_pod_logs(pod_name, int(run_id))

    def delete_pod(self, pod_name) -> bool:
        cmd = "kubectl --namespace={} delete pod {}".format(self.namespace, pod_name)
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)

        if p.returncode > 0:
            logging.critical("Can't delete pod {}, output={}, stderr: {}".format(pod_name, p.stdout, p.stderr))
            return  False
        return True

    def get_kube_exec_cmd(self, cmd) -> str:
        return 'kubectl --namespace={} exec {} -- bash -c {}'.format(self.namespace, self.master_pod, pipes.quote(cmd))

    def get_kube_create_cmd(self, cmd, run_id) -> str:
        container_hash = hashlib.md5(cmd.encode('utf-8')).hexdigest()
        podfile_name = os.path.dirname(__file__)+"/../../Resources/files/pod-{}.yaml".format(container_hash)
        with open(os.path.dirname(__file__)+"/../../Resources/files/pod-tpl.yaml", "r") as file:
            tpl = file.read()

        res =  tpl.format(
            namespace = self.namespace,
            pod_prefix = self.pod_prefix,
            run_id = run_id,
            image = '311239890978.dkr.ecr.eu-central-1.amazonaws.com/base:nextflow-dev-latest',
            container_hash = container_hash,
            run_remote_dir = '/var/run/secrets/kubernetes.io/serviceaccount',
            claim_name = self.volume,
            instance_name = self.hostname,
            last_connect = int(float(time.time())),
            agent_id = self.agent_id,
            cmd =  cmd
        )
        with open(podfile_name, 'w') as file:
            file.write(res)

        cmd = "kubectl create -f {}".format(podfile_name)
        self.master_pod = "{}-{}".format(self.pod_prefix, container_hash)
        return cmd

    def _create_folder_remote(self, folder: str):
        folder = self.work_dir + '/' + folder
        logging.info("Creating folder {}".format(folder))
        cmd = 'mkdir -p {}'.format(pipes.quote(folder))
        [code, output, err] = self._exec_cmd_remote(cmd)
        if code > 0:
            logging.critical("Can't create folder {}, output={}, stderr: {}".format(folder, output, err))

    def _upload_file(self, content: str, filename: str):
        filename = self.work_dir + '/' + filename
        tmp = tempfile.NamedTemporaryFile(delete=False, prefix="upload_tmp_file", suffix=".bin", mode='w')
        logging.info("Uploading file {} to remote {}".format(tmp.name, filename))
        tmp.write(content)
        tmp.close()
        cmd = 'kubectl --namespace={} cp {} {}:{}'.format(self.namespace, tmp.name, self.master_pod, filename)
        args = shlex.split(cmd)
        logging.info("Executing command: {}".format(cmd))
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0:
            logging.critical("Can't upload file {}, stdout={}, error={}".format(filename, p.stdout, p.stderr))

        logging.debug("stdout={}, err={}".format(p.stdout, p.stderr))

    def _get_upload_file_to_s3_cmd(self, folder: string, remote_file_name: string, s3_path: string) -> string:
        cmd = 'cd {}; export AWS_CONFIG_FILE=aws_config; export AWS_SHARED_CREDENTIALS_FILE=aws_credentials; aws s3 ' \
              'cp {} {}'.format(folder, remote_file_name, s3_path)
        return cmd

    def _upload_file_to_s3(self, folder: string, remote_file_name: string, s3_path: string):
        logging.info("Uploading file {} to {}".format(folder+'/'+remote_file_name, s3_path))
        cmd = self._get_upload_file_to_s3_cmd(string, remote_file_name, s3_path)
        [code, output, err] = self._exec_cmd_remote(cmd)
        if code > 0:
            logging.error("Cant upload file to s3, error={}, output={}".format(err, output))


    def _exec_cmd_remote(self, cmd: str) -> [int, str]:
        cmd_wrapped = self.get_kube_exec_cmd(cmd)
        logging.info("Executing command: {}".format(cmd))
        args = shlex.split(cmd_wrapped)
        p = subprocess.run(args, capture_output=True)
        code = p.returncode
        output = p.stdout
        err = p.stderr

        return [code, output, err]

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
