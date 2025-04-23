from datetime import datetime

from src.ThreadsManager.Worker import Worker

class ThreadInfo:
    pid: int = None
    created_at: datetime = None
    worker: Worker = None

    def __init__(self, pid: int, worker: Worker):
        self.pid = pid
        self.created_at = datetime.now()
        self.worker = worker

    def get_created_at_diff(self) -> int:
        now = datetime.now()
        diff = now - self.created_at

        return int(diff.total_seconds())