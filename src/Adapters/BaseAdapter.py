from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun


class BaseAdapter:
    def process_nextflow_run(self, message: NextflowRun) -> bool:
        pass

    def process_get_process_logs(self, message: GetProcessLogs) -> bool:
        pass

    def process_kill_job(self, message: KillJob) -> bool:
        pass

    def type(self):
        pass

    def check_runs_statuses(self):
        pass
