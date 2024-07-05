import csv
import logging
import os

from typing import List, Dict

from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Service.UCDMResolver import UCDMResolver, UCDMConvertedField
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.UCDM.DataSchema import DataSchema


class OMOPofication(WorkflowBase):
    resolver: UCDMResolver
    dir: str = "var/"
    api: Api
    schema: DataSchema
    may_upload_private_data: bool

    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema, may_upload_private_data: bool):
        self.may_upload_private_data = may_upload_private_data
        super().__init__(api, adapter, schema)

    def execute(self, message: StartOMOPoficationWorkflow):
        self.resolver = UCDMResolver(self.api, self.schema)
        logging.info("Workflow execution task")
        logging.info(message)
        length = len(message.queries.items())
        step = 0

        s3_folder = 's3://' + message.s3_bucket + message.s3_path
        result_path = s3_folder if self.may_upload_private_data else (os.path.abspath('.') + '/' + self.dir)

        self.send_notification_to_api(id=message.id, length=length, step=step, state='process', path=result_path)

        try:
            for table_name, val in message.queries.items():
                query = val['query']
                fields_map = val['fieldsMap']
                if table_name == "":
                    table_name = "person"
                self.send_notification_to_api(id=message.id, length=length, step=step, state='process', path=result_path)
                step += 1
                ucdm = self.resolver.get_ucdm_result(query)
                if ucdm is None:
                    logging.error("Can't export {}".format(table_name))
                    continue

                if len(ucdm) > 0:
                    filename = self.build(table_name, ucdm, fields_map)
                    if self.may_upload_private_data:
                        s3_path = s3_folder + table_name + '.csv'
                        if not self.adapter.upload_local_file_to_s3(filename, s3_path, message.aws_id, message.aws_key):
                            logging.critical("Can't upload result file to S3, abort pipeline execution")
                            self.send_notification_to_api(message.id, length, step, 'error', path=result_path)
                            return
                self.send_notification_to_api(id=message.id, length=length, step=step, state='process',
                                              path=result_path)

            self.send_notification_to_api(id=message.id, length=length, step=step, state='success', path=result_path)
        except Exception as e:
            logging.error("Can't finish export, sending error")
            self.send_notification_to_api(message.id, length, step, 'error', path=result_path)
            raise e
        logging.info("Writing OMOP CSV files finished successfully")

    def send_notification_to_api(self, id: int, length: int, step: int, state: str, path: str):
        percent = int(round(step / length * 100, 0))
        self.api.set_job_state(run_id=str(id), state=state, percent=percent, path=path)

    def build(self, table_name: str, ucdm: List[Dict[str, UCDMConvertedField]], fields_map: Dict[str, str]) -> str:
        filename = self.dir + "{}.csv".format(table_name)
        with open(filename, 'w', newline='') as file:
            header = list(fields_map.values())
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                for key, val in row.items():
                    value = val.export_value
                    field_name = fields_map[key]
                    output[field_name] = value if value is not None else ''

                writer.writerow(output)
        return filename
