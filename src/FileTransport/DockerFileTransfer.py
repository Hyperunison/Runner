import os
import shutil
from src.FileTransport.BaseFileTransport import BaseFileTransport
from src.Service.K8s import K8s


def get_local_file_path(origin_file_path: str) -> str:
    return origin_file_path.replace('{home}', os.curdir)


class DockerFileTransfer(BaseFileTransport):
    base_dir: str

    def __init__(self, work_dir: str):
        super().__init__()
        self.base_dir = work_dir.replace('{home}', os.getcwd()) + "/"

    def init(self, run_id: int, agent_id: int):
        pass

    def upload(self, local_path: str, remote_path: str):
        if os.path.isdir(local_path):
            shutil.copytree(local_path, self.base_dir + '/' + remote_path)
            return
        if os.path.isfile(local_path):
            shutil.copy(local_path, self.base_dir + '/' + remote_path)
            return
        raise Exception("Unknown file type {}".format(local_path))

    def mkdir(self, dirname: str):
        os.makedirs(self.base_dir + dirname, exist_ok=True)

    def download(self, remote_path: str, local_path: str):
        if os.path.isdir(self.base_dir + '/' + remote_path):
            shutil.copytree(self.base_dir + '/' + remote_path, local_path)
            return
        if os.path.isfile(self.base_dir + '/' + remote_path):
            shutil.copy(self.base_dir + '/' + remote_path, local_path)
            return
        raise Exception("Unknown file type {}".format(local_path))

    def cleanup(self):
        pass
