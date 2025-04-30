import importlib
import logging
import os
import traceback

from src import Api
from src.Message.StartWorkflow import StartWorkflow
from src.Service.Workflows import PipelineExecutor
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.UCDM import DataSchema
import importlib.util


class ExternalPipeline(WorkflowBase):
    module = None

    def __init__(self, api: Api, pipeline_executor: PipelineExecutor, schema: DataSchema, filename: str):
        super().__init__(api, pipeline_executor, schema)
        module_name = os.path.splitext(os.path.basename(filename))[0]
        spec = importlib.util.spec_from_file_location(module_name, filename)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        # self.module = importlib.import_module(filename)

    def execute(self, message: StartWorkflow, api: Api):
        logging.info("Workflow execution task")
        logging.info(message)
        logging.info("Parameters: {}".format(message.parameters))
        self.api.set_run_status(message.run_id, 'deploy')

        ucdm = self.get_ucdm(message)
        ucdm_simplified = [{key: val.export_value for key, val in row.items()} for row in ucdm]

        try:
            input_files = self.module.get_input_files(ucdm_simplified, message.parameters)
            output_file_masks = self.module.get_output_file_masks(message.parameters)
            cmd = self.module.get_nextflow_cmd(input_files, message.parameters, message.run_name, message.weblog_url)

            self.pipeline_executor.run_nextflow_run_abstract(
                message.run_id,
                cmd,
                message.dir,
                input_files,
                output_file_masks,
                message.s3_path,
                message.aws_id,
                message.aws_key
            )
        except Exception:
            error = traceback.format_exc()
            logging.critical("Pipeline unknown exception occurred", exc_info=True)
            self.api.set_run_status(message.run_id, 'error')
            self.api.add_log_chunk(message.run_id, error)

