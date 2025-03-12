import json
import logging
import os
from typing import List, Dict

from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Service.Workflows import PipelineExecutor
from src.Service.Workflows.OMOPification.CsvWritter import CsvWritter
from src.Service.Workflows.OMOPification.PostgresqlExporter import PostgresqlExporter
from src.Service.Workflows.OMOPification.SQLiteExporter import SQLiteExporter
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Api import Api
from src.Message.partial.CohortDefinition import CohortDefinition
from src.Service.Csv.CsvToMappingTransformer import CsvToMappingTransformer
from src.Service.UCDMMappingResolver import UCDMMappingResolver
from src.Service.UCDMResolver import UCDMResolver
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.UCDM.DataSchema import DataSchema
from src.Service.ApiLogger import ApiLogger
from src.Service.Workflows.StrToIntGenerator import StrToIntGenerator

class OMOPofication(WorkflowBase):
    resolver: UCDMResolver
    dir: str = "var/"
    manual_file_name: str = "var/manual.pdf"
    manual_csv_file_name: str = "var/manual.csv"
    api: Api

    def __init__(self, api: Api, pipeline_executor: PipelineExecutor, schema: DataSchema, may_upload_private_data: bool):
        self.may_upload_private_data = may_upload_private_data
        super().__init__(api, pipeline_executor, schema)

    def execute(self, message: StartOMOPoficationWorkflow, api: Api):
        api_logger = ApiLogger(self.api)
        self.download_mapping()
        csv_transformer = CsvToMappingTransformer()
        api_logger.write(message.id, "Workflow execution task")
        logging.info(message)
        length = len(message.queries.items())
        step = 0

        automation_strategies_map = message.automation_strategies_map

        filename = os.path.abspath(self.dir + '/automation_strategies_map.json')
        with open(filename, 'w') as file:
            json.dump(automation_strategies_map, file, indent=4)

        s3_folder = 's3://' + message.s3_bucket + message.s3_path
        result_path = s3_folder if self.may_upload_private_data else (os.path.abspath('.') + '/' + self.dir)

        if self.may_upload_private_data:
            if not self.pipeline_executor.adapter.upload_local_file_to_s3(
                os.path.abspath(self.dir+'/automation_strategies_map.json'),
                s3_folder + 'automation_strategies_map.json',
                message.aws_id,
                message.aws_key,
                False
            ):
                api_logger.write(message.id, "Can't upload automation_strategies_map.json file to S3")

            if not self.pipeline_executor.adapter.upload_local_file_to_s3(
                    os.path.abspath(self.mapping_file_name),
                    s3_folder + 'mapping-values.csv',
                    message.aws_id,
                    message.aws_key,
                    False
            ):
                api_logger.write(message.id, "Can't upload mapping-values.csv file to S3")

        self.send_notification_to_api(id=message.id, length=length, step=step, state='process', path=result_path)
        self.download_manual_pdf(s3_folder, message, api_logger)
        self.download_manual_csv(s3_folder, message, api_logger)

        str_to_int = StrToIntGenerator()
        str_to_int.load_from_file()
        try:
            if message.format == 'postgresql':
                exporter = PostgresqlExporter(
                    connection_string=message.connection_string
                )
                exporter.create_all_tables(message.all_tables)

            if message.format == 'sqlite':
                exporter = SQLiteExporter()
                exporter.create_all_tables(message.all_tables)

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
                    str_to_int,
                    fields_map,
                    automation_strategies_map
                )
                self.save_sql_query(table_name, sql_final, s3_folder, message, api_logger)
                if ucdm is None:
                    api_logger.write(message.id, "Can't export {}".format(table_name))
                    continue
                api_logger.write(message.id, "Harmonized rows count: {}".format(len(ucdm)))

                if len(ucdm) > 0:
                    if message.format in ('postgresql', 'sqlite'):
                        skipped_rows = self.save_rows_to_database(
                            table_name=table_name,
                            ucdm=ucdm,
                            fields_map=fields_map,
                            connection_string=message.connection_string,
                            columns=self.get_columns(
                                table_name=table_name,
                                tables=message.all_tables
                            ),
                            format=message.format
                        )
                        api_logger.write(message.id, "Table {} was filled".format(table_name))
                    else:
                        filename = self.dir + "{}.csv".format(table_name)
                        skipped_rows = self.build_csv_file(filename, ucdm, fields_map)
                        if self.may_upload_private_data:
                            s3_path = s3_folder + table_name + '.csv'
                            if not self.pipeline_executor.adapter.upload_local_file_to_s3(filename, s3_path, message.aws_id, message.aws_key, False):
                                api_logger.write(message.id, "Can't upload result file to S3, abort pipeline execution")
                                self.send_notification_to_api(message.id, length, step, 'error', path=result_path)
                                return
                        api_logger.write(message.id, "{}.csv file was written".format(table_name))

                    # if len(skipped_rows) > 0:
                    #     api_logger.write(message.id, '\n'.join(skipped_rows))
            self.send_notification_to_api(id=message.id, length=length, step=step, state='success', path=result_path)
            str_filename = str_to_int.save_to_file()

            if not self.pipeline_executor.adapter.upload_local_file_to_s3(
                    os.path.abspath(str_filename),
                    s3_folder + 'str-to-int.csv',
                    message.aws_id,
                    message.aws_key,
                    False
            ):
                api_logger.write(message.id, "Can't upload str-to-int.csv file to S3")

            if self.may_upload_private_data and message.format == 'sqlite':
                exporter = SQLiteExporter()
                self.pipeline_executor.adapter.upload_local_file_to_s3(
                    os.path.abspath(exporter.file_name),
                    s3_folder + exporter.file_name,
                    message.aws_id,
                    message.aws_key,
                    False
                )

        except Exception as e:
            api_logger.write(message.id, "ERROR: Can't finish export, sending error {}".format(','.join(e.args)))
            self.send_notification_to_api(message.id, length, step, 'error', path=result_path)
            raise e
        api_logger.write(message.id, "Writing OMOP CSV files finished successfully")

    def download_manual_pdf(self, s3_folder: str, message: StartOMOPoficationWorkflow, api_logger: ApiLogger):
        response = self.api.export_mapping_docs()
        with open(os.path.abspath(self.manual_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)

        if self.may_upload_private_data:
            s3_path = s3_folder + 'manual.pdf'
            if not self.pipeline_executor.adapter.upload_local_file_to_s3(self.manual_file_name, s3_path, message.aws_id, message.aws_key, False):
                api_logger.write(message.id, "Can't upload manual.pdf file to S3")
                return

    def download_manual_csv(self, s3_folder: str, message: StartOMOPoficationWorkflow, api_logger: ApiLogger):
        response = self.api.export_mapping_docs_csv()
        with open(os.path.abspath(self.manual_csv_file_name), 'wb') as file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                file.write(chunk)

        if self.may_upload_private_data:
            s3_path = s3_folder + 'manual.csv'
            if not self.pipeline_executor.adapter.upload_local_file_to_s3(self.manual_csv_file_name, s3_path, message.aws_id, message.aws_key, False):
                api_logger.write(message.id, "Can't upload manual.csv file to S3")
                return

    def save_sql_query(self, table_name: str, query: str, s3_folder: str, message: StartOMOPoficationWorkflow, api_logger: ApiLogger):
        filename = os.path.abspath(self.dir + table_name + ".sql")
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(query)

        if self.may_upload_private_data:
            s3_path = s3_folder + table_name + '.sql'
            if not self.pipeline_executor.adapter.upload_local_file_to_s3(filename, s3_path, message.aws_id, message.aws_key, False):
                api_logger.write(message.id, "Can't upload {}.sql file to S3".format(table_name))
                return

    def save_fields_map(self, fields_map, table_name: str):
        filename = os.path.abspath(self.dir + table_name + "-fields-map.json")
        with open(filename, 'w') as file:
            json.dump(fields_map, file, indent=4)


    def send_notification_to_api(self, id: int, length: int, step: int, state: str, path: str):
        percent = int(round(step / length * 100, 0))
        self.api.set_job_state(run_id=str(id), state=state, percent=percent, path=path)

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
            connection_string: str,
            columns: List[Dict[str, str]],
            format: str
    ) -> List[str]:
        if format == 'sqlite':
            exporter = SQLiteExporter()

            return exporter.export(
                table_name=table_name,
                ucdm=ucdm,
                fields_map=fields_map,
                columns=columns
            )
        else:
            exporter = PostgresqlExporter(
                connection_string=connection_string
            )

            return exporter.export(
                table_name=table_name,
                ucdm=ucdm,
                fields_map=fields_map,
                columns=columns
            )

    def get_columns(self, table_name: str, tables: List[Dict[str, any]]) -> List[Dict[str, str]]:
        for table in tables:
            if table['tableName'] == table_name:
                return table['columns']
