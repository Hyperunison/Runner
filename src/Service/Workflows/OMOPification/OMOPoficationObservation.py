from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv

from src.Service.Workflows.PersonIdGenerator import PersonIdGenerator


class OMOPoficationObservation(OMOPoficationBase):
    def build(self, ucdm: List[Dict[str, UCDMConvertedField]], person_id_generator: PersonIdGenerator):
        header = ["observation_id", "person_id", "observation_concept_id", "observation_date", "observation_datetime",
                  "observation_type_concept_id", "value_as_number", "value_as_string", "value_as_concept_id",
                  "qualifier_concept_id", "unit_concept_id", "provider_id", "visit_occurrence_id", "visit_detail_id",
                  "observation_source_value", "observation_source_concept_id", "unit_source_value", "qualifier_source_value"]
        filename = self.dir + "/observation.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            num: int = 1
            for row in ucdm:
                output = {}
                output["observation_id"] = str(num)
                output["person_id"] = person_id_generator.get_person_id_int(row['participant_id'].biobank_value)
                output["observation_concept_id"] = self.render_omop_id(row, 'c.name')
                output["observation_date"] = self.render_ucdm_value(row, 'c.date')
                output["observation_datetime"] = self.render_ucdm_value(row, 'c.datetime')
                output["observation_type_concept_id"] = self.render_omop_id(row, 'c.type')
                output["value_as_number"] = self.render_biobank_value(row, 'c.value_as_string')
                output["value_as_string"] = self.render_biobank_value(row, 'c.value_as_number')
                output["value_as_concept_id"] = self.render_omop_id(row, 'c.value')
                output["qualifier_concept_id"] = self.render_omop_id(row, 'c.qualifier')
                output["unit_concept_id"] = self.render_omop_id(row, 'c.unit')
                output["provider_id"] = self.render_omop_id(row, 'c.provider')
                output["visit_occurrence_id"] = self.render_ucdm_value(row, 'c.visit_occurrence')
                output["visit_detail_id"] = self.render_ucdm_value(row, 'c.visit_detail')
                output["observation_source_value"] = ""
                output["observation_source_concept_id"] = ""
                output["unit_source_value"] = self.render_biobank_value(row, 'c.unit')
                output["qualifier_source_value"] = ""
                num += 1

                writer.writerow(output)