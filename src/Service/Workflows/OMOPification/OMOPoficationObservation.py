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
            for row in ucdm:
                output = {}
                output["observation_id"] = ""
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["observation_concept_id"] = ""
                output["observation_date"] = ""
                output["observation_datetime"] = ""
                output["observation_type_concept_id"] = ""
                output["value_as_number"] = ""
                output["value_as_string"] = ""
                output["value_as_concept_id"] = ""
                output["qualifier_concept_id"] = ""
                output["unit_concept_id"] = ""
                output["provider_id"] = ""
                output["visit_occurrence_id"] = ""
                output["visit_detail_id"] = ""
                output["observation_source_value"] = ""
                output["observation_source_concept_id"] = ""
                output["unit_source_value"] = ""
                output["qualifier_source_value"] = ""

                writer.writerow(output)