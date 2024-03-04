from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationProcedure(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, str]]):
        header = ["procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date",
                  "procedure_datetime", "procedure_type_concept_id", "modifier_concept_id",
                  "quantity", "provider_id", "visit_occurrence_id", "visit_detail_id",
                  "procedure_source_value", "procedure_source_concept_id", "modifier_source_value"]
        filename = self.dir + "/procedure.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                writer.writerow(output)