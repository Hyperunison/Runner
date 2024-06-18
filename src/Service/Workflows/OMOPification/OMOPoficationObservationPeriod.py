from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv

from src.Service.Workflows.PersonIdGenerator import PersonIdGenerator


class OMOPoficationObservationPeriod(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, UCDMConvertedField]], person_id_generator: PersonIdGenerator):
        header = ["observation_period_id", "person_id", "observation_period_start_date",
                  "observation_period_end_date", "period_type_concept_id"]
        filename = self.dir + "/observation_period.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["observation_period_id"] = ""
                output["person_id"] = person_id_generator.get_person_id_int(row['participant_id'].biobank_value)
                output["observation_period_start_date"] = self.render_ucdm_value(row, 'c.start_date')
                output["observation_period_end_date"] = self.render_ucdm_value(row, 'c.end_date')
                output["period_type_concept_id"] = self.render_omop_id(row, 'c.type')
                writer.writerow(output)