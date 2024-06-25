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

    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema):
        super().__init__(api, adapter, schema)

    def execute(self, message: StartOMOPoficationWorkflow):
        self.resolver = UCDMResolver(self.api, self.schema)
        logging.info("Workflow execution task")
        logging.info(message)
        length = len(message.queries.items())
        step = 0

        self.send_notification_to_api(id=message.id, length=length, step=step)

        for table_name, val in message.queries.items():
            query = val['query']
            fields_map = val['fieldsMap']
            if table_name == "":
                table_name = "person"
            step += 1
            ucdm = self.resolver.get_ucdm_result(query)
            if ucdm is None:
                logging.error("Can't export {}".format(table_name))
                continue

            if len(ucdm) > 0:
                self.build(table_name, ucdm, fields_map)

            self.send_notification_to_api(id=message.id, length=length, step=step)
        logging.info("Writing OMOP CSV files finished successfully")

    def send_notification_to_api(self, id: int, length: int, step: int):
        percent = int(round(step / length * 100, 0))
        state = 'process'
        if percent == 100:
            state = 'success'
            self.dir = os.getcwd() + "/" + self.dir
        self.api.set_job_state(run_id=str(id), state=state, percent=percent, path=self.dir)

    def build(self, table_name: str, ucdm: List[Dict[str, UCDMConvertedField]], fields_map: Dict[str, str]):
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
