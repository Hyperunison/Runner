from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationObservation(OMOPoficationBase):
    filename: str
    error_file: str
    header = ["observation_id", "person_id", "observation_concept_id", "observation_date", "observation_datetime",
              "observation_type_concept_id", "value_as_number", "value_as_string", "value_as_concept_id",
              "qualifier_concept_id", "unit_concept_id", "provider_id", "visit_occurrence_id", "visit_detail_id",
              "observation_source_value", "observation_source_concept_id", "unit_source_value",
              "qualifier_source_value"]

    def build(self, ucdm: List[Dict[str, str]]):
        self.filename = self.dir + "/observation.csv"
        self.error_file = self.error_report_dir + "/observation.csv"
        with open(self.filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                if not self.is_row_valid(row):
                    self.add_row_to_error_rows(row)
                    continue

                output = self.compose_output_row(row)
                writer.writerow(output)
        self.save_error_rows()

    def is_row_valid(self, row):
        return True

    def save_error_rows(self):
        if len(self.error_rows) == 0:
            return

        with open(self.error_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.header)
            writer.writeheader()

            for row in self.error_rows:
                output = self.compose_output_row(row)
                writer.writerow(output)

    def compose_output_row(self, row):
        output = {}
        output["observation_id"] = ""
        output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
        output["observation_concept_id"] = ""
        output["observation_date"] = row['c.date'].ucdm_value
        output["observation_datetime"] = ""
        output["observation_type_concept_id"] = row['c.name'].biobank_value
        output["value_as_number"] = ""
        output["value_as_string"] = row['c.value'].biobank_value
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

        return output