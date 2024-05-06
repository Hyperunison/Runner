from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationSpecimen(OMOPoficationBase):
    filename: str
    error_file: str
    error_rows: List[Dict]
    header = ["specimen_id", "person_id", "specimen_concept_id", "specimen_type_concept_id",
              "specimen_date", "specimen_datetime", "quantity", "unit_concept_id",
              "anatomic_site_concept_id", "disease_status_concept_id", "specimen_source_id",
              "specimen_source_value", "unit_source_value", "anatomic_site_source_value",
              "disease_status_source_value"]

    def build(self, ucdm: List[Dict[str, str]]):
        self.filename = self.dir + "/specimen.csv"
        self.error_file = self.error_report_dir + "/specimen.csv"
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
        output["specimen_id"] = ""
        output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
        output["specimen_concept_id"] = row['c.name'].biobank_value
        output["specimen_type_concept_id"] = row['c.type'].biobank_value
        output["specimen_date"] = row['c.date'].biobank_value
        output["specimen_datetime"] = ""
        output["quantity"] = ""
        output["unit_concept_id"] = ""
        output["anatomic_site_concept_id"] = ""
        output["disease_status_concept_id"] = ""
        output["specimen_source_id"] = ""
        output["specimen_source_value"] = ""
        output["unit_source_value"] = ""
        output["anatomic_site_source_value"] = ""
        output["disease_status_source_value"] = ""

        return output