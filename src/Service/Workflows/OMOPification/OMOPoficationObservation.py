from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationObservation(OMOPoficationBase):
    def build(self, ucdm: List[Dict[str, str]]):
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
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["observation_concept_id"] = row['c.name'].omop_id if 'c.name' in row else ''
                output["observation_date"] = row['c.date'].ucdm_value if 'c.date' in row else ''
                output["observation_datetime"] = row['c.datetime'].ucdm_value if 'c.datetime' in row else ''
                output["observation_type_concept_id"] = row['c.type'].omop_id if 'c.type' in row else ''
                output["value_as_number"] = row['c.value_as_string'].biobank_value if 'c.value_as_string' in row else ''
                output["value_as_string"] = row['c.value_as_number'].biobank_value if 'c.value_as_number' in row else ''
                output["value_as_concept_id"] = row['c.value'].omop_id if 'c.value' in row else ''
                output["qualifier_concept_id"] = row['c.qualifier'].omop_id if 'c.qualifier' in row else ''
                output["unit_concept_id"] = row['c.unit'].omop_id if 'c.unit' in row else ''
                output["provider_id"] = row['c.provider'].omop_id if 'c.provider' in row else ''
                output["visit_occurrence_id"] = row['c.visit_occurrence'].ucdm_value if 'c.visit_occurrence' in row else ''
                output["visit_detail_id"] = row['c.visit_detail'].ucdm_value if 'c.visit_detail' in row else ''
                output["observation_source_value"] = ""
                output["observation_source_concept_id"] = ""
                output["unit_source_value"] = row['c.unit'].biobank_value if 'c.unit' in row else ''
                output["qualifier_source_value"] = ""
                num += 1

                writer.writerow(output)