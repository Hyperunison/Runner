import logging
from typing import Optional

from src.Message import StartOMOPoficationWorkflow
from src.Service.Workflows.OMOPification import BaseDatabaseExporter
from src.Service.Workflows.OMOPification.CSVExporter import CSVExporter
from src.Service.Workflows.OMOPification.PostgresqlExporter import PostgresqlExporter
from src.Service.Workflows.OMOPification.SQLiteExporter import SQLiteExporter
from src.Service.Workflows.OMOPification.XPTExporter import XPTExporter


def exporter_factory(
        message: StartOMOPoficationWorkflow,
        concept_path: Optional[str] = None,
        vocabulary_path: Optional[str] = None
) -> BaseDatabaseExporter:
    logging.info("Exporter type: {}".format(message.format))
    if message.format == 'postgresql':
        exporter = PostgresqlExporter(
            connection_string=message.connection_string
        )

        return setup_server_data_csv_files(
            exporter,
            message,
            concept_path,
            vocabulary_path
        )

    if message.format == 'sqlite':
        exporter = SQLiteExporter()

        return setup_server_data_csv_files(
            exporter,
            message,
            concept_path,
            vocabulary_path
        )

    if message.format == 'xpt':
        return XPTExporter()

    return CSVExporter()

def setup_server_data_csv_files(
        exporter: BaseDatabaseExporter,
        message: StartOMOPoficationWorkflow,
        concept_path: Optional[str] = None,
        vocabulary_path: Optional[str] = None
) -> BaseDatabaseExporter:
    if message.does_server_data_omop_concept_exist():
        exporter.concept_csv_path = concept_path

    if message.does_server_data_omop_vocabularies_exist():
        exporter.vocabulary_csv_path = vocabulary_path

    return exporter