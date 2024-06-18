from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv

from src.Service.Workflows.PersonIdGenerator import PersonIdGenerator


class OMOPoficationDeath(OMOPoficationBase):
    def build(self, ucdm: List[Dict[str, UCDMConvertedField]], person_id_generator: PersonIdGenerator):
        header = ["person_id", "death_date", "death_datetime", "death_type_concept_id",
                  "cause_concept_id", "cause_source_value", "cause_source_concept_id"]
        filename = self.dir + "/death.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["person_id"] = person_id_generator.get_person_id_int(row['participant_id'].biobank_value)
                output["death_date"] = self.render_ucdm_value(row, 'c.date')
                output["death_datetime"] = self.render_ucdm_value(row, 'c.datetime')
                output["death_type_concept_id"] = self.render_omop_id(row, 'c.type')
                output["cause_concept_id"] = self.render_omop_id(row, 'c.cause')
                output["cause_source_value"] = self.render_biobank_value(row, 'c.cause')
                output["cause_source_concept_id"] = ""
                writer.writerow(output)