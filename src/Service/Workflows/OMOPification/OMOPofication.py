import csv
import json
import logging
import os
from typing import List, Dict

from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Service.Workflows.OMOPification.CsvWritter import CsvWritter
from src.Service.Workflows.OMOPification.PostgresqlExporter import PostgresqlExporter
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Api import Api
from src.Message.partial.CohortDefinition import CohortDefinition
from src.Service.Csv.CsvToMappingTransformer import CsvToMappingTransformer
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.UCDMResolverTwo import UCDMResolver
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Adapters.BaseAdapter import BaseAdapter
from src.UCDM.DataSchema import DataSchema, VariableMapper
from src.Service.ApiLogger import ApiLogger
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator

class OMOPofication(WorkflowBase):
    resolver: UCDMResolver
    dir: str = "var/"
    mapping_file_name: str = "var/mapping-values.csv"
    manual_file_name: str = "var/manual.pdf"
    api: Api

    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema, may_upload_private_data: bool):
        self.may_upload_private_data = may_upload_private_data
        super().__init__(api, adapter, schema)

    def execute(self, message: StartOMOPoficationWorkflow):
        api_logger = ApiLogger(self.api)
        self.download_mapping()
        csv_transformer = CsvToMappingTransformer()
        api_logger.write(message.id, "Workflow execution task")
        logging.info(message)
        length = len(message.queries.items())
        step = 0

        s3_folder = 's3://' + message.s3_bucket + message.s3_path
        result_path = s3_folder if self.may_upload_private_data else (os.path.abspath('.') + '/' + self.dir)

        if self.may_upload_private_data:
            s3_path = s3_folder + 'mapping-values.csv'
            if not self.adapter.upload_local_file_to_s3(os.path.abspath(self.mapping_file_name), s3_path, message.aws_id, message.aws_key):
                api_logger.write(message.id, "Can't upload mapping-values.csv file to S3")

        self.send_notification_to_api(id=message.id, length=length, step=step, state='process', path=result_path)
        self.download_manual(s3_folder, message, api_logger)

        str_to_int = StrToIntGenerator()
        str_to_int.load_from_file()
        try:
            for table_name, val in message.queries.items():
                api_logger.write(message.id, "Start exporting {}".format(table_name))
                query = CohortDefinition(val['query'])
                fields_map = val['fieldsMap']
                if table_name == "":
                    table_name = "person"
                self.save_fields_map(fields_map, table_name)
                self.send_notification_to_api(
                    id=message.id,
                    length=length,
                    step=step,
                    state='process',
                    path=result_path
                )
                step += 1
                sql_final = self.get_sql_final(query)
                csv_mapping = csv_transformer.transform_with_file_path(
                    os.path.abspath(self.mapping_file_name),
                    table_name,
                )
                self.mapping_resolver = UCDMMappingResolver(csv_mapping)
                self.resolver = UCDMResolver(self.schema, self.mapping_resolver)
                ucdm = self.resolver.get_ucdm_result(
                    sql_final,
                    str_to_int
                )
                self.save_sql_query(table_name, sql_final, s3_folder, message, api_logger)
                if ucdm is None:
                    api_logger.write(message.id, "Can't export {}".format(table_name))
                    continue
                api_logger.write(message.id, "Harmonized rows count: {}".format(len(ucdm)))

                if len(ucdm) > 0:
                    if message.format == 'postgresql':
                        skipped_rows = self.save_rows_to_database(
                            table_name,
                            ucdm,
                            fields_map,
                            message.connection_string
                        )
                        api_logger.write(message.id, "Table {} was filled".format(table_name))
                    else:
                        filename = self.dir + "{}.csv".format(table_name)
                        skipped_rows = self.build_csv_file(filename, ucdm, fields_map)
                        if self.may_upload_private_data:
                            s3_path = s3_folder + table_name + '.csv'
                            if not self.adapter.upload_local_file_to_s3(filename, s3_path, message.aws_id, message.aws_key):
                                api_logger.write(message.id, "Can't upload result file to S3, abort pipeline execution")
                                self.send_notification_to_api(message.id, length, step, 'error', path=result_path)
                                return
                        api_logger.write(message.id, "{}.csv file was written".format(table_name))

                    if len(skipped_rows) > 0:
                        api_logger.write(message.id, '\n'.join(skipped_rows))
            self.send_notification_to_api(id=message.id, length=length, step=step, state='success', path=result_path)
            str_to_int.save_to_file()

        except Exception as e:
            api_logger.write(message.id, "ERROR: Can't finish export, sending error {}".format(','.join(e.args)))
            self.send_notification_to_api(message.id, length, step, 'error', path=result_path)
            raise e
        api_logger.write(message.id, "Writing OMOP CSV files finished successfully")

    def download_mapping(self):
        response = self.api.export_mapping()
        with open(os.path.abspath(self.mapping_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)

    def download_manual(self, s3_folder: str, message: StartOMOPoficationWorkflow, api_logger: ApiLogger):
        response = self.api.export_mapping_docs()
        with open(os.path.abspath(self.manual_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)

        if self.may_upload_private_data:
            s3_path = s3_folder + 'manual.pdf'
            if not self.adapter.upload_local_file_to_s3(self.manual_file_name, s3_path, message.aws_id, message.aws_key):
                api_logger.write(message.id, "Can't upload manual.pdf file to S3")
                return

    def save_sql_query(self, table_name: str, query: str, s3_folder: str, message: StartOMOPoficationWorkflow, api_logger: ApiLogger):
        filename = os.path.abspath(self.dir + table_name + ".sql")
        with open(filename, 'wb') as file:
            file.write(bytes(query, 'utf-8'))

        if self.may_upload_private_data:
            s3_path = s3_folder + table_name + '.sql'
            if not self.adapter.upload_local_file_to_s3(filename, s3_path, message.aws_id, message.aws_key):
                api_logger.write(message.id, "Can't upload {}.sql file to S3".format(table_name))
                return

    def save_fields_map(self, fields_map, table_name: str):
        filename = os.path.abspath(self.dir + table_name + "-fields-map.json")
        with open(filename, 'w') as file:
            json.dump(fields_map, file, indent=4)


    def send_notification_to_api(self, id: int, length: int, step: int, state: str, path: str):
        percent = int(round(step / length * 100, 0))
        self.api.set_job_state(run_id=str(id), state=state, percent=percent, path=path)

    def get_sql_final(self, cohort_definition: CohortDefinition) -> str:
        mapper = VariableMapper(cohort_definition.fields)

        return self.schema.build_cohort_definition_sql_query(
            mapper,
            cohort_definition,
            False,
            False,
        )

    def build_csv_file(
            self,
            filename: str,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        csv_writter = CsvWritter()

        return csv_writter.build(filename, ucdm, fields_map)

    def save_rows_to_database(
            self,
            table_name: str,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]],
            connection_string: str
    ) -> List[str]:
        exporter = PostgresqlExporter(
            connection_string=connection_string,
            table_name=table_name
        )

        return exporter.export(ucdm, fields_map)
