from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationVisitOccurrence(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, str]]):
        header = ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date",
                  "visit_start_datetime", "visit_end_date", "visit_end_datetime", "visit_type_concept_id",
                  "provider_id", "care_site_id", "visit_source_value", "visit_source_concept_id",
                  "admitting_source_concept_id", "admitting_source_value", "discharge_to_concept_id",
                  "discharge_to_source_value", "preceding_visit_occurrence_id"]
        filename = self.dir + "/visit_occurrence.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["visit_occurrence_id"] = ""
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["visit_concept_id"] = ""
                output["visit_start_date"] = row['c.start_date'].ucdm_value
                output["visit_start_datetime"] = ""
                output["visit_end_date"] = row['c.end_date'].ucdm_value
                output["visit_end_datetime"] = ""
                output["visit_type_concept_id"] = ""
                output["provider_id"] = ""
                output["care_site_id"] = ""
                output["visit_source_value"] = ""
                output["visit_source_concept_id"] = ""
                output["admitting_source_concept_id"] = row['c.admitting'].biobank_value
                output["admitting_source_value"] = row['c.admitting'].ucdm_value
                output["discharge_to_concept_id"] = row['c.discharge'].biobank_value
                output["discharge_to_source_value"] = row['c.discharge'].ucdm_value
                output["preceding_visit_occurrence_id"] = ""
                writer.writerow(output)