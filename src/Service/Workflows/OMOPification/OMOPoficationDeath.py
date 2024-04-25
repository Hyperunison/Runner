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
                output["death_date"] = row['c.date'].ucdm_value if 'c.date' in row else ''
                output["death_datetime"] = row['c.datetime'].ucdm_value if 'c.datetime' in row else ''
                output["death_type_concept_id"] = row['c.type'].omop_id if 'c.type' in row else ''
                output["cause_concept_id"] = row['c.cause'].omop_id if 'c.cause' in row else ''
                output["cause_source_value"] = row['c.cause'].biobank_value if 'c.cause' in row else ''
                output["cause_source_concept_id"] = ""
                writer.writerow(output)