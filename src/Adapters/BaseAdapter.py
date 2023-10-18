from typing import Optional, Dict

from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun


class BaseAdapter:
    def process_nextflow_run(self, message: NextflowRun) -> bool:
        pass

    def run_nextflow_run_abstract(
        self, run_id: int, nextflow_command: str, dir: Optional[str], aws_s3_path: str, input_files: Dict[str, str], output_files: Dict[str, str]
    ) -> bool:
        pass

    def process_get_process_logs(self, message: GetProcessLogs) -> bool:
        pass

    def process_kill_job(self, message: KillJob) -> bool:
        pass

    def type(self):
        pass

    def check_runs_statuses(self):
        pass
