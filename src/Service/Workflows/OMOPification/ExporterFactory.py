from src.Message import StartOMOPoficationWorkflow
from src.Service.Workflows.OMOPification import BaseDatabaseExporter
from src.Service.Workflows.OMOPification.CSVExporter import CSVExporter
from src.Service.Workflows.OMOPification.PostgresqlExporter import PostgresqlExporter
from src.Service.Workflows.OMOPification.SQLiteExporter import SQLiteExporter


def exporter_factory(message: StartOMOPoficationWorkflow) -> BaseDatabaseExporter:
    if message.format == 'postgresql':
        return PostgresqlExporter(
            connection_string=message.connection_string
        )

    if message.format == 'sqlite':
        return SQLiteExporter()

    return CSVExporter()