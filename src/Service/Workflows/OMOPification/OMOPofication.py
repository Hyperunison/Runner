import json
import logging
from typing import List, Dict

from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Message.StartWorkflow import StartWorkflow
from src.Service.UCDMResolver import UCDMResolver
from src.Service.Workflows.WorkflowBase import WorkflowBase
from src.Adapters.BaseAdapter import BaseAdapter
from src.Api import Api
from src.UCDM.DataSchema import DataSchema
import csv


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
                return
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
        header = ["person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime",
                  "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id",
                  "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value",
                  "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"]
        filename = self.dir + "/person.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["person_id"] = row['participant_id'].biobank_value
                output["gender_concept_id"] = row['gender'].omop_id
                output["year_of_birth"] = row['year_of_birth'].ucdm_value
                output["month_of_birth"] = ""                               # todo: calculate
                output["day_of_birth"] = ""                                 # todo: calculate
                output["birth_datetime"] = ""                               # todo: calculate
                output["race_concept_id"] = row['race'].omop_id
                output["ethnicity_concept_id"] = row['ethnicity'].omop_id
                output["location_id"] = ""                                  # todo
                output["provider_id"] = ""                                  # todo
                output["care_site_id"] = ""                                 # todo
                output["person_source_value"] = row['participant_id'].biobank_value
                output["gender_source_value"] = row['gender'].ucdm_value
                output["gender_source_concept_id"] = ""
                output["race_source_value"] = row['race'].ucdm_value
                output["race_source_concept_id"] = ""
                output["ethnicity_source_value"] = row['ethnicity'].ucdm_value
                output["ethnicity_source_concept_id"] = ""
                writer.writerow(output)

    def build_condition_table(self, ucdm: List[Dict[str, str]]):
        header = ["condition_occurrence_id", "person_id", "condition_concept_id", "condition_start_date",
                  "condition_start_datetime", "condition_end_date", "condition_end_datetime", "condition_type_concept_id",
                  "condition_status_concept_id", "stop_reason", "provider_id", "visit_occurrence_id", "visit_detail_id",
                  "condition_source_value", "condition_source_concept_id", "condition_status_source_value"]
        filename = self.dir + "/person.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["condition_occurrence_id"] = ""
                output["person_id"] = row['participant_id'].biobank_value
                output["condition_concept_id"] = ""
                output["condition_start_date"] = ""
                output["condition_start_datetime"] = ""
                output["condition_end_date"] = ""
                output["condition_end_datetime"] = ""
                output["condition_type_concept_id"] = ""
                output["condition_status_concept_id"] = ""
                output["stop_reason"] = ""
                output["provider_id"] = ""
                output["visit_occurrence_id"] = ""
                output["visit_detail_id"] = ""
                output["condition_source_value"] = ""
                output["condition_source_concept_id"] = ""
                output["condition_status_source_value"] = ""

                writer.writerow(output)

    def build_measurement_table(self, ucdm: List[Dict[str, str]]):
        pass

    def build_drug_expose_table(self, ucdm: List[Dict[str, str]]):
        pass

    def build_observation_period_table(self, ucdm: List[Dict[str, str]]):
        pass

    def build_procedure_table(self, ucdm: List[Dict[str, str]]):
        pass

    def build_visit_occurrence_table(self, ucdm: List[Dict[str, str]]):
        pass


