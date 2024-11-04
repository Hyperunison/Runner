import json
import logging
import os
import string
import tempfile
import random
from typing import Dict, Optional
import yaml
from src.Adapters import BaseAdapter
from src.FileTransport.FileTransferFactory import create_file_transfer
from src.Message.NextflowRun import NextflowRun
from src.Api import Api


def get_pipeline_remote_dir_name(work_dir: str) -> str:
    if work_dir == "" or work_dir is None:
        return 'pipeline_' + _random_word(16)
    else:
        return work_dir


class PipelineExecutor:
    adapter: BaseAdapter
    api_client: Api
    config: Dict
    agent_id: int

    def __init__(self, adapter: BaseAdapter, api_client: Api, config: Dict, agent_id: int):
        self.adapter = adapter
        self.api_client = api_client
        self.config = config
        self.agent_id = agent_id

    def process_nextflow_run(self, message: NextflowRun):
        input_files: Dict[str, str] = {
            'main.nf': message.nextflow_code,
            'data.json': json.dumps(message.input_data),
            'nextflow.config': file_get_contents('nextflow.config'),
        }

        output_file_masks: Dict[str, str] = {
            ".nextflow.log": "/basic/",
            "trace-*.txt": "/basic/",
        }

        self.run_nextflow_run_abstract(
            message.run_id,
            message.command,
            message.dir,
            message.aws_s3_path,
            input_files,
            output_file_masks,
            message.aws_id,
            message.aws_key
        )

    def run_nextflow_run_abstract(
            self, run_id: int, nextflow_command: str, workdir: Optional[str], aws_s3_path: str,
            input_files: Dict[str, str], output_file_masks: Dict[str, str], aws_id: str, aws_key: str
    ):
        self.api_client.set_run_status(run_id, 'process')
        file_transfer = create_file_transfer(self.config['file_transfer'])

        self.api_client.api_instance.api_client.close()
        self.api_client.api_instance.api_client.rest_client.pool_manager.clear()
        folder = get_pipeline_remote_dir_name(workdir)

        logging.info("Uploading workflow files")

        self.api_client.set_run_dir(run_id, folder)

        file_transfer.init(run_id, self.agent_id)

        file_transfer.mkdir(folder)

        for file in input_files.keys():
            file_transfer.upload(create_temp_file(input_files[file]), folder + "/" + file)

        file_transfer.cleanup()

        os.mkdir("var/" + folder)

        pipeline_config: Dict = {
            'aws_id': aws_id,
            'aws_key': aws_key,
            'aws_s3_path': aws_s3_path,
            'output_file_masks': output_file_masks,
        }

        file_put_contents('var/{}/pipeline_config.yaml'.format(folder), yaml.dump(pipeline_config))

        self.adapter.run_pipeline(nextflow_command, folder, run_id)

    def check_runs_statuses(self):
        self.adapter.check_runs_statuses()


def file_get_contents(file_path: str) -> str:
    with open(file_path, 'r') as file:
        return file.read()


def create_temp_file(content: str) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, prefix="upload_tmp_file", suffix=".bin", mode='w')
    tmp.write(content)
    tmp.close()
    return tmp.name


def _random_word(length: int):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def file_put_contents(file_path: str, content: str):
    with open(file_path, 'w') as file:
        file.write(content)
