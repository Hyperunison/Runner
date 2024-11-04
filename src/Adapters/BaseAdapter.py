import logging
import shlex
import subprocess
from typing import Optional, Dict

from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun


class BaseAdapter:
    def process_nextflow_run(self, message: NextflowRun, config: Dict) -> bool:
        pass

    def run_nextflow_run_abstract(
            self, run_id: int, nextflow_command: str, dir: Optional[str], aws_s3_path: str, input_files: Dict[str, str],
            output_files: Dict[str, str], aws_id: str, aws_key: str
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
        logging.info("Uploading local file {} to {}".format(filename, s3_path))

        cmd = (
            'bash -c \'export AWS_ACCESS_KEY_ID="{}"; export AWS_SECRET_ACCESS_KEY="{}"; aws s3 cp --recursive {} {}\''.
            format(aws_id, aws_key, filename, s3_path))

        logging.info("Executing command: {}".format(cmd))
        p = subprocess.run(shlex.split(cmd), capture_output=True)

        if p.returncode > 0:
            logging.critical(
                "Can't upload file to S3, stdout={}, error={}, return_code={}".format(cmd, p.stdout, p.stderr,
                                                                                      p.returncode))
            return False

        return True
