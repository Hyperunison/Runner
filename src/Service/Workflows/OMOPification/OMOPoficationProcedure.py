from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationProcedure(OMOPoficationBase):
    filename: str
    error_file: str
    error_rows: List[Dict]
    header = ["procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date",
              "procedure_datetime", "procedure_type_concept_id", "modifier_concept_id",
              "quantity", "provider_id", "visit_occurrence_id", "visit_detail_id",
              "procedure_source_value", "procedure_source_concept_id", "modifier_source_value"]
    def build(self, ucdm: List[Dict[str, str]]):
        self.filename = self.dir + "/procedure.csv"
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
        output["procedure_occurrence_id"] = ""
        output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
        output["procedure_concept_id"] = row['c.name'].omop_id
        output["procedure_date"] = self.transform_float_to_date(row['c.date'].biobank_value)
        output["procedure_datetime"] = self.transform_float_to_datetime(row['c.date'].biobank_value)
        output["procedure_type_concept_id"] = ""
        output["modifier_concept_id"] = ""
        output["quantity"] = ""
        output["provider_id"] = ""
        output["visit_occurrence_id"] = ""
        output["visit_detail_id"] = ""
        output["procedure_source_value"] = row['c.value'].biobank_value
        output["procedure_source_concept_id"] = ""
        output["modifier_source_value"] = ""

        return output