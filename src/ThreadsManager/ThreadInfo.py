from datetime import datetime


class ThreadInfo:
    pid: int = None
    created_at: datetime = None
    future = None

    def __init__(self, pid: int):
        self.pid = pid
        self.created_at = datetime.now()

    def get_created_at_diff(self) -> int:
        now = datetime.now()
        diff = now - self.created_at

        return int(diff.total_seconds())