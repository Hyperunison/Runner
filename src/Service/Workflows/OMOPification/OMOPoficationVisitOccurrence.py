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
                writer.writerow(output)