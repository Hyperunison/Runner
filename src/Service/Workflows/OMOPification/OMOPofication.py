import csv
import logging
import os

from typing import List, Dict

from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Service.ApiLogger import ApiLogger
from src.Message.partial.CohortDefinition import CohortDefinition
from src.Service.Csv.ListToCsvTransformer import ListToCsvTransformer
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.UCDMResolver import UCDMResolver, UCDMConvertedField
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.UCDM.DataSchema import DataSchema


class OMOPofication(WorkflowBase):
    resolver: UCDMResolver
    mapping_resolver: UCDMMappingResolver
    dir: str = "var/"
    mapping_file_name: str = "var/mapping-values.csv"
    api: Api
    schema: DataSchema
    may_upload_private_data: bool

    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema, may_upload_private_data: bool):
        self.may_upload_private_data = may_upload_private_data
        super().__init__(api, adapter, schema)

    def execute(self, message: StartOMOPoficationWorkflow):
        api_logger = ApiLogger(self.api)
        self.mapping_resolver = UCDMMappingResolver(self.api)
        self.download_mapping()
        self.resolver = UCDMResolver(self.api, self.schema)
        self.resolver.set_ucdm_mapping_resolver(self.mapping_resolver)
        api_logger.write(message.id, "Workflow execution task")
        logging.info(message)
        length = len(message.queries.items())
        step = 0

        s3_folder = 's3://' + message.s3_bucket + message.s3_path
        result_path = s3_folder if self.may_upload_private_data else (os.path.abspath('.') + '/' + self.dir)

        self.send_notification_to_api(id=message.id, length=length, step=step, state='process', path=result_path)

        str_to_int = StrToIntGenerator()
        try:
            for table_name, val in message.queries.items():
                api_logger.write(message.id, "Start exporting {}".format(table_name))
                query = CohortDefinition(val['query'])
                fields_map = val['fieldsMap']
                if table_name == "":
                    table_name = "person"
                self.send_notification_to_api(id=message.id, length=length, step=step, state='process',
                                              path=result_path)
                step += 1
                ucdm = self.resolver.get_ucdm_result(query, api_logger, message.id, str_to_int)
                if ucdm is None:
                    api_logger.write(message.id, "Can't export {}".format(table_name))
                    continue
                api_logger.write(message.id, "Harmonized rows count: {}".format(len(ucdm)))

                if len(ucdm) > 0:
                    filename = self.dir + "{}.csv".format(table_name)
                    self.build(filename, ucdm, fields_map, message.id, api_logger)
                    api_logger.write(message.id, "{}.csv file was written".format(table_name))
                    if self.may_upload_private_data:
                        s3_path = s3_folder + table_name + '.csv'
                        if not self.adapter.upload_local_file_to_s3(filename, s3_path, message.aws_id, message.aws_key):
                            api_logger.write(message.id, "Can't upload result file to S3, abort pipeline execution")
                            self.send_notification_to_api(message.id, length, step, 'error', path=result_path)
                            return
                self.send_notification_to_api(id=message.id, length=length, step=step, state='process',
                                              path=result_path)

            self.send_notification_to_api(id=message.id, length=length, step=step, state='success', path=result_path)
        except Exception as e:
            api_logger.write(message.id, "ERROR: Can't finish export, sending error {}".format(','.join(e.args)))
            self.send_notification_to_api(message.id, length, step, 'error', path=result_path)
            raise e
        api_logger.write(message.id, "Writing OMOP CSV files finished successfully")

    def download_mapping(self):
        response = self.api.export_mapping_json()
        transformer = ListToCsvTransformer()
        transformer.convert(
            response,
            os.path.abspath(self.mapping_file_name)
        )

    def send_notification_to_api(self, id: int, length: int, step: int, state: str, path: str):
        percent = int(round(step / length * 100, 0))
        self.api.set_job_state(run_id=str(id), state=state, percent=percent, path=path)

    def build(
            self,
            filename: str,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]],
            runner_message_id: int,
            api_logger: ApiLogger
    ):
        with open(filename, 'w', newline='') as file:
            header = [item['name'] for item in fields_map.values()]
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()
            skip_rows: List[str] = []
            for row in ucdm:
                output = {}
                skip_reasons: List[str] = []
                for key, val in row.items():
                    value = val.export_value
                    if (value == '' or value is None or value == '0' or value == 0) and fields_map[key]['isRequired']:
                        skip_reasons.append("{} is empty, but required (value={})".format(key, value))
                    field_name = fields_map[key]['name']
                    output[field_name] = value if value is not None else ''

                if len(skip_reasons) == 0:
                    writer.writerow(output)
                else:
                    row_str: Dict[str, str] = {}
                    for k, v in row.items():
                        row_str[k] = v.export_value
                    skip_rows.append("Skip row as [{}]. Row={}".format(", ".join(skip_reasons), str(row_str)))
            if len(skip_rows) > 0:
                api_logger.write(runner_message_id, '\n'.join(skip_rows))
