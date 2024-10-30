from src.FileTransport import BaseFileTransport
from src.FileTransport.K8sFileTransfer import K8sFileTransfer


def create_file_transfer(config: dict[str, any]) -> BaseFileTransport:
    if config['type'] == 'k8s':
        return K8sFileTransfer(
            config['namespace'], config['image'], config['command'], config['pod_prefix'], config['base_dir'],
            config['volumes'], config['labels']
        )

    raise Exception("Unknown file transfer type {}".format(config['type']))

