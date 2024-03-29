from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationCondition(OMOPoficationBase):
    def build(self, ucdm: List[Dict[str, str]]):
        header = ["condition_occurrence_id", "person_id", "condition_concept_id", "condition_start_date",
                  "condition_start_datetime", "condition_end_date", "condition_end_datetime",
                  "condition_type_concept_id",
                  "condition_status_concept_id", "stop_reason", "provider_id", "visit_occurrence_id", "visit_detail_id",
                  "condition_source_value", "condition_source_concept_id", "condition_status_source_value"]
        filename = self.dir + "/condition_occurrence.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["condition_occurrence_id"] = ""
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["condition_concept_id"] = row['c.icd10'].omop_id
                output["condition_start_date"] = row['c.start_date'].ucdm_value
                output["condition_start_datetime"] = ""
                output["condition_end_date"] = row['c.end_date'].ucdm_value
                output["condition_end_datetime"] = ""
                output["condition_type_concept_id"] = ""
                output["condition_status_concept_id"] = ""
                output["stop_reason"] = row['c.stop_reason'].ucdm_value
                output["provider_id"] = ""
                output["visit_occurrence_id"] = ""
                output["visit_detail_id"] = ""
                output["condition_source_value"] = ""
                output["condition_source_concept_id"] = ""
                output["condition_status_source_value"] = ""

                writer.writerow(output)