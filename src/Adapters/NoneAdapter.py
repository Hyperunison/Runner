import logging
import random
import shlex
import string
import subprocess
import time
import os
import sys
from typing import Dict
import yaml

from ..FileTransport.FileTransferFactory import create_file_transfer
from ..Service.K8s import K8s as K8sService
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.Message import KillJob
from src.Message.GetProcessLogs import GetProcessLogs


class NoneAdapter(BaseAdapter):
    def type(self):
        return 'none'

    def run_pipeline(self, nextflow_command: str, folder: str, run_id: int):
        pass

    def process_get_process_logs(self, message: GetProcessLogs):
        return

    def process_kill_job(self, message: KillJob) -> bool:
        return True

    def process_send_pod_logs(self, k8s: K8sService, pod_name: string, run_id: int):
        pass

    def check_runs_statuses(self):
        return
