from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationPerson(OMOPoficationBase):
    filename: str
    error_file: str
    error_rows: List[Dict]
    header = ["person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime",
              "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id",
              "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value",
              "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"]

    def build(self, ucdm: List[Dict[str, str]]):
        self.filename = self.dir + "/person.csv"
        self.error_file = self.error_report_dir + "/person.csv"
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
        output["gender_concept_id"] = row['gender'].omop_id
        output["year_of_birth"] = row['year_of_birth'].ucdm_value
        output["month_of_birth"] = ""  # todo: calculate
        output["day_of_birth"] = ""  # todo: calculate
        output["birth_datetime"] = ""  # todo: calculate
        output["race_concept_id"] = row['race'].omop_id
        output["ethnicity_concept_id"] = row['ethnicity'].omop_id
        output["location_id"] = ""  # todo
        output["provider_id"] = ""  # todo
        output["care_site_id"] = ""  # todo
        output["person_source_value"] = row['participant_id'].biobank_value
        output["gender_source_value"] = row['gender'].ucdm_value
        output["gender_source_concept_id"] = ""
        output["race_source_value"] = row['race'].ucdm_value
        output["race_source_concept_id"] = ""
        output["ethnicity_source_value"] = row['ethnicity'].ucdm_value
        output["ethnicity_source_concept_id"] = ""

        return output