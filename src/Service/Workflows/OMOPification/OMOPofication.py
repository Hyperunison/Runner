import logging
from typing import List, Dict

from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Service.UCDMResolver import UCDMResolver
from src.Service.Workflows.OMOPification.OMOPoficationCondition import OMOPoficationCondition
from src.Service.Workflows.OMOPification.OMOPoficationDrugExpose import OMOPoficationDrugExpose
from src.Service.Workflows.OMOPification.OMOPoficationMeasurement import OMOPoficationMeasurement
from src.Service.Workflows.OMOPification.OMOPoficationObservationPeriod import OMOPoficationObservationPeriod
from src.Service.Workflows.OMOPification.OMOPoficationPerson import OMOPoficationPerson
from src.Service.Workflows.OMOPification.OMOPoficationProcedure import OMOPoficationProcedure
from src.Service.Workflows.OMOPification.OMOPoficationVisitOccurrence import OMOPoficationVisitOccurrence
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.UCDM.DataSchema import DataSchema


class OMOPofication(WorkflowBase):
    resolver: UCDMResolver
    dir: str = "var/"

    def __init__(self, api: Api, adapter: BaseAdapter, schema: DataSchema):
        super().__init__(api, adapter, schema)
        self.resolver = UCDMResolver(api, schema)

    def execute(self, message: StartOMOPoficationWorkflow):
        logging.info("Workflow execution task")
        logging.info(message)

        for table_name, query in message.queries.items():
            ucdm = self.resolver.get_ucdm_result(query)
            if len(ucdm) == 0:
                continue
            if table_name == "":
                self.build_person_table(ucdm)
            elif table_name == "condition":
                self.build_condition_table(ucdm)
            elif table_name == "measurement":
                self.build_measurement_table(ucdm)
            elif table_name == "drug_expose":
                self.build_drug_expose_table(ucdm)
            elif table_name == "observation_period":
                self.build_observation_period_table(ucdm)
            elif table_name == "procedure":
                self.build_procedure_table(ucdm)
            elif table_name == "visit_occurrence":
                self.build_visit_occurrence_table(ucdm)
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

    def build_drug_expose_table(self, ucdm: List[Dict[str, str]]):
        builder = OMOPoficationDrugExpose()
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


