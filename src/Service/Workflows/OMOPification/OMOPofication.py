import logging
import os

from typing import List, Dict

from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Service.UCDMResolver import UCDMResolver
from src.Service.Workflows.OMOPification.OMOPoficationCondition import OMOPoficationCondition
from src.Service.Workflows.OMOPification.OMOPoficationDrugExposure import OMOPoficationDrugExposure
from src.Service.Workflows.OMOPification.OMOPoficationMeasurement import OMOPoficationMeasurement
from src.Service.Workflows.OMOPification.OMOPoficationObservationPeriod import OMOPoficationObservationPeriod
from src.Service.Workflows.OMOPification.OMOPoficationPerson import OMOPoficationPerson
from src.Service.Workflows.OMOPification.OMOPoficationProcedure import OMOPoficationProcedure
from src.Service.Workflows.OMOPification.OMOPoficationVisitOccurrence import OMOPoficationVisitOccurrence
from src.Service.Workflows.OMOPification.OMOPoficationDeath import OMOPoficationDeath
from src.Service.Workflows.OMOPification.OMOPoficationObservation import OMOPoficationObservation
from src.Service.Workflows.OMOPification.OMOPoficationSpecimen import OMOPoficationSpecimen
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.UCDM.DataSchema import DataSchema


class OMOPofication(WorkflowBase):
    resolver: UCDMResolver
    dir: str = ""

    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema):
        super().__init__(api, adapter, schema)
        self.resolver = UCDMResolver(api, schema)

    def execute(self, message: StartOMOPoficationWorkflow):
        logging.info("Workflow execution task")
        logging.info(message)
        length = len(message.queries.items())
        step = 0

        for table_name, query in message.queries.items():
            ucdm = self.resolver.get_ucdm_result(query)
            if ucdm is None:
                logging.error("Can't export {}".format(table_name))
                continue

            if len(ucdm) > 0:
                if table_name == "":
                    self.build_person_table(ucdm)
                elif table_name == "condition":
                    self.build_condition_table(ucdm)
                elif table_name == "measurement":
                    self.build_measurement_table(ucdm)
                elif table_name == "drug_exposure":
                    self.build_drug_exposure_table(ucdm)
                elif table_name == "observation_period":
                    self.build_observation_period_table(ucdm)
                elif table_name == "procedure":
                    self.build_procedure_table(ucdm)
                elif table_name == "visit_occurrence":
                    self.build_visit_occurrence_table(ucdm)
                elif table_name == "death":
                    self.build_death_table(ucdm)
                elif table_name == "observation":
                    self.build_observation_table(ucdm)
                elif table_name == "specimen":
                    self.build_specimen_table(ucdm)

            step = step + 1
            self.send_notification_to_api(
                id=message.id,
                length=length,
                step=step
            )
        print("OK")

    def build_person_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationPerson()
        builder.build(ucdm)

    def build_condition_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationCondition()
        builder.build(ucdm)

    def build_measurement_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationMeasurement()
        builder.build(ucdm)

    def build_drug_exposure_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationDrugExposure()
        builder.build(ucdm)

    def build_observation_period_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationObservationPeriod()
        builder.build(ucdm)

    def build_procedure_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationProcedure()
        builder.build(ucdm)

    def build_visit_occurrence_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationVisitOccurrence()
        builder.build(ucdm)

    def build_death_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationDeath()
        builder.build(ucdm)

    def build_observation_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationObservation()
        builder.build(ucdm)

    def build_specimen_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationSpecimen()
        builder.build(ucdm)

    def send_notification_to_api(self, id: int, length: int, step: int):
        percent = int(round(step / length * 100, 0))
        state = 'process'
        if percent == 100:
            state = 'success'
            self.dir = os.getcwd() + "/" + OMOPoficationPerson().get_dir()
        self.api.set_job_state(
            run_id=str(id),
            state=state,
            percent=percent,
            path=self.dir
        )

