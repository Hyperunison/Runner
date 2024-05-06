from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationDeath(OMOPoficationBase):
    filename: str
    error_file: str
    header = ["person_id", "death_date", "death_datetime", "death_type_concept_id",
              "cause_concept_id", "cause_source_value", "cause_source_concept_id"]

    def build(self, ucdm: List[Dict[str, str]]):
        self.filename = self.dir + "/death.csv"
        self.error_file = self.error_report_dir + "/death.csv"
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
        output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
        output["death_date"] = ""
        output["death_datetime"] = ""
        output["death_type_concept_id"] = ""
        output["cause_concept_id"] = row['c.cause'].biobank_value
        output["cause_source_value"] = ""
        output["cause_source_concept_id"] = ""

        return output