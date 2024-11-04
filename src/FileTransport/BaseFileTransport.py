
class BaseFileTransport:
    def __init__(self):
        pass

    def init(self, run_id: int, agent_id: int):
        pass

    def cleanup(self):
        pass

    def upload(self, local_path: str, remote_path: str):
        pass

    def mkdir(self, dirname: str):
        pass

    def download(self, remote_path: str, local_path: str):
        pass