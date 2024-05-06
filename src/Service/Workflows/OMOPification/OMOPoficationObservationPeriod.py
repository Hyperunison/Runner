from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationObservationPeriod(OMOPoficationBase):
    filename: str
    error_file: str
    header = ["observation_period_id", "person_id", "observation_period_start_date",
              "observation_period_end_date", "period_type_concept_id"]
    def build(self, ucdm: List[Dict[str, str]]):
        self.filename = self.dir + "/observation_period.csv"
        self.error_file = self.dir + "/observation_period.csv"

        with open(self.filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                if not self.is_row_valid(row):
                    self.add_row_to_error_rows(row)
                    continue

                output = self.compose_output_row(row)
                writer.writerow(output)

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
        output["observation_period_id"] = ""
        output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
        output["observation_period_start_date"] = ""
        output["observation_period_end_date"] = ""
        output["period_type_concept_id"] = ""

        return output