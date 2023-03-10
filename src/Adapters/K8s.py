import json
import logging
import pipes
import random
import shlex
import string
import subprocess
import tempfile
import time

from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message.NextflowRun import NextflowRun

sendLogsPeriod = 3


class K8s(BaseAdapter):
    namespace: str = None
    master_pod: str = None
    work_dir: str = None
    api_client: Api = None

    def __init__(self, api_client: Api, work_dir: str, config):
        self.namespace = config['namespace']
        self.master_pod = config['master_pod']
        self.api_client = api_client
        self.work_dir = work_dir

    def type(self):
        return 'k8s'

    def process_nextflow_run(self, message: NextflowRun) -> bool:
        # create folder
        # upload data.json and main.nf

        folder = message.dir
        if folder == "" or folder is None:
            folder = 'tmp_' + self._random_word(16)

        cmd = self.get_kube_exec_cmd('cd {}; {}'.format(self.work_dir + '/' + folder, message.command))

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

        args = shlex.split(cmd)
        logging.info("Executing command: {}".format(cmd))
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.api_client.set_run_status(message.run_id, 'process')

        last_send = time.perf_counter()
        buffer = ''
        while line := p.stdout.readline().decode("utf-8"):
            logging.info('Stdout line: {}'.format(line.strip()))
            buffer += line
            if time.perf_counter() - last_send > sendLogsPeriod and buffer != '':
                self.api_client.add_log_chunk(message.run_id, buffer)
                buffer = ''
                last_send = time.perf_counter()

        logging.info("Socket finished")
        p.wait()
        if buffer != '':
            self.api_client.add_log_chunk(message.run_id, buffer)

        logging.info("Exit code={}".format(p.returncode))
        if p.returncode == 0:
            self.api_client.set_run_status(message.run_id, 'success')
        else:
            self.api_client.set_run_status(message.run_id, 'error')

        return True

    def get_kube_exec_cmd(self, cmd) -> str:
        return 'kubectl --namespace={} exec {} -- bash -c {}'.format(self.namespace, self.master_pod, pipes.quote(cmd))

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
        p = subprocess.run(args)
        if p.returncode > 0:
            logging.critical("Can't upload file {}, stdout={}, error={}".format(filename, p.stdout, p.stderr))

        logging.debug("stdout={}, err={}".format(p.stdout, p.stderr))

    def _exec_cmd_remote(self, cmd: str) -> [int, str]:
        cmd_wrapped = self.get_kube_exec_cmd(cmd)
        logging.info("Executing command: {}".format(cmd))
        args = shlex.split(cmd_wrapped)
        p = subprocess.run(args)
        code = p.returncode
        output = p.stdout
        err = p.stderr

        return [code, output, err]

    def _random_word(self, length: int):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))
