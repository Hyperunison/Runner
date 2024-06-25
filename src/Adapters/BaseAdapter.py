from typing import Optional, Dict

from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun


class BaseAdapter:
    def process_nextflow_run(self, message: NextflowRun) -> bool:
        pass

    def run_nextflow_run_abstract(
            self, run_id: int, nextflow_command: str, dir: Optional[str], aws_s3_path: str, input_files: Dict[str, str],
            output_files: Dict[str, str]
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

    def upload_local_file_to_s3(self, filename: str, s3_path: str, aws_id: str, aws_key: str) -> bool:
        pass

    def get_aws_config_file_content(self) -> str:
        return "[default]\nregion = eu-central-1\n"

    def get_aws_credentials_file_content(self, aws_id: str, aws_key: str) -> str:
        return "[default]\naws_access_key_id={}\naws_secret_access_key={}\n".format(aws_id, aws_key)
