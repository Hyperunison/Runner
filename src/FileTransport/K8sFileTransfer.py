import time

from src.FileTransport.BaseFileTransport import BaseFileTransport
from src.Service.K8s import K8s


class K8sFileTransfer(BaseFileTransport):
    pod_name: str
    namespace: str
    image: str
    command: str
    pod_prefix: str
    base_dir: str
    volumes: dict[str, str]
    labels: dict[str, str]
    k8s: K8s

    def __init__(self, namespace: str, image: str, command: str, pod_prefix: str, base_dir: str,
                 volumes: dict[str, str], labels: dict[str, str]):
        super().__init__()
        self.namespace = namespace
        self.image = image
        self.command = command
        self.pod_prefix = pod_prefix
        self.base_dir = base_dir
        self.volumes = volumes
        self.labels = labels

        self.k8s = K8s(namespace)

    def init(self, run_id: int, agent_id: int):
        labels = self.get_labels(run_id, agent_id)
        pod_name = self.k8s.create_pod(self.pod_prefix, self.command, labels, self.image, self.volumes)
        if pod_name is None:
            raise Exception("Can't create pod for uploading files")
        self.pod_name = pod_name

        pod_started = self.k8s.wait_pod_status(self.pod_name, ['Running'], 60)
        if not pod_started:
            raise Exception("Can't start pod for uploading files")

        self.k8s.wait_pod_status(pod_name, ['Running'])

    def get_labels(self, run_id: int, agent_id: int):
        labels = self.labels
        for label in labels.keys():
            labels[label] = labels[label].replace('{pod_prefix}', self.pod_prefix)
            labels[label] = labels[label].replace('{run_id}', str(run_id))
            labels[label] = labels[label].replace('{last_connect}', str(int(float(time.time()))))
            labels[label] = labels[label].replace('{agent_id}', str(agent_id))

        return labels

    def upload(self, local_path: str, remote_path: str):
        self.k8s.upload(self.pod_name, local_path, self.base_dir + '/' + remote_path)

    def mkdir(self, dirname: str):
        self.k8s.create_folder_remote(self.pod_name, self.base_dir + '/' + dirname)

    def download(self, remote_path: str, local_path: str):
        self.k8s.download(self.pod_name, self.base_dir + "/" + remote_path, local_path)

    def cleanup(self):
        self.k8s.delete_pod(self.pod_name)
