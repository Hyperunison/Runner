from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationDeath(OMOPoficationBase):
    def build(self, ucdm: List[Dict[str, str]]):
        header = ["person_id", "death_date", "death_datetime", "death_type_concept_id",
                  "cause_concept_id", "cause_source_value", "cause_source_concept_id"]
        filename = self.dir + "/death.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["death_date"] = ""
                output["death_datetime"] = ""
                output["death_type_concept_id"] = ""
                output["cause_concept_id"] = ""
                output["cause_source_value"] = ""
                output["cause_source_concept_id"] = ""
                writer.writerow(output)