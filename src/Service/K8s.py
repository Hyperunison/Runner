import random
import json
import logging
import pipes
import shlex
import string
import subprocess
import time
import hashlib
import os
from json import JSONDecodeError
from typing import List, Optional, Dict
import yaml

sendLogsPeriod = 3

class K8s:
    namespace: str = None

    def __init__(self, namespace: str):
        self.namespace = namespace

    def get_pod_logs(self, pod_name: str, lines: int = 100) -> str:
        cmd = "kubectl --namespace={} logs --tail {} {}".format(self.namespace, lines, pod_name)
        logging.info("Executing command: {}".format(cmd))
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)

        if p.returncode == 0:
            logs = str(p.stdout.decode('utf-8'))
        else:
            logs = "Can't get pod logs"
            logging.error(
                "Can't get logs, stdout={}, error={}".format(p.stdout.decode('utf-8'), p.stderr.decode('utf-8')))

        return logs

    def search_pods(self, selectors: Dict[str, str], field_selectors: Dict[str, str]) -> List[Dict]:
        cmd = "kubectl --namespace={} -o json get pods".format(self.namespace)
        for key in selectors.keys():
            cmd = cmd + (' --selector={}={}'.format(key, selectors[key]))
        for key in field_selectors.keys():
            cmd = cmd + (' --field-selector={}={}'.format(key, field_selectors[key]))
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0:
            logging.critical("Can't find pod, output={}, stderr: {}".format(p.stdout, p.stderr))
            raise Exception("Cant execute search pods")
        data = json.loads(str(p.stdout.decode('utf-8')))
        result: List[Dict] = []
        return data['items']

    def subscribe_pod_logs(self, pod_name: string, callback):
        cmd = "kubectl --namespace={} logs {} -c {} -f" . format(self.namespace, pod_name, pod_name)
        args = shlex.split(cmd)
        logging.info("Executing command: {}" . format(cmd))
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        last_send = time.perf_counter()
        buffer = ''
        while line := p.stdout.readline().decode("utf-8"):
            logging.info('Stdout line: {}' . format(line.strip()))
            buffer += line
            if time.perf_counter() - last_send > sendLogsPeriod and buffer != '':
                callback(buffer)
                buffer = ''
                last_send = time.perf_counter()

        logging.info("Socket finished")

        if buffer != '':
            callback(buffer)

    def add_label_to_pod(self, pod_name: str, label: str, override=False):
        cmd = "kubectl --namespace={} label pods {} {} {}".format(self.namespace, pod_name, label,
                                                                  ('--overwrite' if override else ''))
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0 or len(p.stderr) > 0:
            logging.critical("Can't add label to pod, output={}, stderr: {}".format(p.stdout, p.stderr))
            return False
        return True

    def delete_pod(self, pod_name) -> bool:
        cmd = "kubectl --namespace={} delete pod {}".format(self.namespace, pod_name)
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)

        if p.returncode > 0 or len(p.stderr) > 0:
            logging.critical("Can't delete pod {}, output={}, stderr: {}".format(pod_name, p.stdout, p.stderr))
            return False
        return True

    def get_kube_exec_cmd(self, pod_name: str, cmd: str) -> str:
        return 'kubectl --namespace={} exec {} -- bash -c {}'.format(self.namespace, pod_name, pipes.quote(cmd))

    def create_pod(
            self, pod_prefix: str, cmd: str, labels: dict[str, str], image: str, volumes: dict[str, str]
    ) -> str:
        container_hash = hashlib.md5(cmd.encode('utf-8') + self._random_word(12).encode('utf-8')).hexdigest()
        podfile_name = os.path.dirname(__file__) + "/../../Resources/files/pod-{}.yaml".format(container_hash)

        pod_name = "{}-{}".format(pod_prefix, container_hash)

        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": self.namespace,
                "labels": labels,
            },

            "spec": {
                "restartPolicy": "Never",
                "containers": [{
                    "command": ["bash", "-c"],
                    "args": [cmd],
                    "image": image,
                    "imagePullPolicy": "Always",
                    "name": pod_name,
                    "resources": {
                        "requests": {
                            "cpu": "2"
                        }
                    },
                    "volumeMounts": []
                }],
                "volumes": []
            }
        }
        for claim in volumes.keys():
            data_name = self._random_word(12)
            pod['spec']['containers'][0]['volumeMounts'].append({
                "name": data_name,
                "mountPath": volumes[claim]
            })
            pod['spec']['volumes'].append({
                "name": data_name,
                "persistentVolumeClaim": {"claimName": claim}
            })

        with open(podfile_name, 'w') as file:
            file.write(yaml.dump(pod, default_flow_style=None, width=70))

        cmd = "kubectl create -f {}".format(podfile_name)

        args = shlex.split(cmd)
        logging.info("Executing command: {}".format(cmd))
        p = subprocess.run(args, capture_output=True)

        if p.returncode > 0 or len(p.stderr) > 0:
            raise Exception(
                "Can't create pod, stdout={}, error={}, return_code={}".format(cmd, p.stdout, p.stderr, p.returncode)
            )

        return pod_name

    def get_pod_status(self, pod_name: str) -> Optional[str]:
        cmd = "kubectl --namespace={} get pod {} -o json".format(self.namespace, pod_name)
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0:
            logging.critical("Can't add label to pod, output={}, stderr: {}".format(p.stdout, p.stderr))
            raise Exception("Can't add label to pod {}".format(pod_name))

        json_string: str = str(p.stdout.decode('utf-8'))
        try:
            data = json.loads(json_string)
            return data['status']['phase']
        except JSONDecodeError as e:
            logging.error("Can't parse k8s response, as it's not json: {}".format(json_string))
            return None

    def wait_pod_status(self, pod_name: str, required_statuses: List[str], timeout: int = 30, interval: int = 1) -> bool:
        total_wait = 0
        while total_wait < timeout:
            status = self.get_pod_status(pod_name)
            if status in required_statuses:
                return True
            time.sleep(interval)
            total_wait += interval

        return False

    def create_folder_remote(self, pod_name: str, folder: str) -> bool:
        logging.info("Creating folder {}".format(folder))
        cmd = 'mkdir -p {}'.format(pipes.quote(folder))
        [code, output, err] = self._exec_cmd_remote(pod_name, cmd)
        if code > 0:
            logging.critical("Can't create folder {}, output={}, stderr: {}".format(folder, output, err))
            return False
        return True

    def upload(self, pod_name: str, local_filename: str, remote_filename: str) -> bool:
        cmd = 'kubectl --namespace={} cp {} {}:"{}"'.format(self.namespace, local_filename, pod_name, remote_filename)
        args = shlex.split(cmd)
        logging.info("Executing command: {}".format(cmd))
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0 or len(p.stderr) > 0:
            logging.critical("Can't upload file {}, stdout={}, error={}".format(local_filename, p.stdout, p.stderr))
            return False

        logging.debug("stdout={}, err={}".format(p.stdout, p.stderr))

        return True

    def download(self, pod_name: str, remote_filename: str, local_filename: str) -> bool:
        cmd = 'kubectl --namespace={} cp {}:"{}" {}'.format(self.namespace, pod_name, remote_filename, local_filename)
        args = shlex.split(cmd)
        logging.info("Executing command: {}".format(cmd))
        p = subprocess.run(args, capture_output=True)
        if p.returncode > 0 or len(p.stderr) > 0:
            logging.critical("Can't download file {}, stdout={}, error={}".format(local_filename, p.stdout, p.stderr))
            return False

        logging.debug("stdout={}, err={}".format(p.stdout, p.stderr))

        return True

    def _exec_cmd_remote(self, pod_name: str, cmd: str) -> [int, str]:
        cmd_wrapped = self.get_kube_exec_cmd(pod_name, cmd)
        logging.info("Executing command: {}".format(cmd_wrapped))
        args = shlex.split(cmd_wrapped)
        p = subprocess.run(args, capture_output=True)
        code = p.returncode
        output = p.stdout
        err = p.stderr

        return [code, output, err]

    def _random_word(self, length: int):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))
