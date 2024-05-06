from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationMeasurement(OMOPoficationBase):
    filename: str
    error_file: str
    header = ["measurement_id", "person_id", "measurement_concept_id", "measurement_date",
              "measurement_datetime", "measurement_time", "measurement_type_concept_id",
              "operator_concept_id", "value_as_number", "value_as_concept_id",
              "unit_concept_id", "range_low", "range_high", "provider_id", "visit_occurrence_id",
              "visit_detail_id", "measurement_source_value", "measurement_source_concept_id",
              "unit_source_value", "value_source_value"]

    def build(self, ucdm: List[Dict[str, str]]):

        filename = self.dir + "/measurement.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
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
                if not self.is_row_valid(row):
                    self.add_row_to_error_rows(row)
                    continue
                    
                output = self.compose_output_row(row)
                writer.writerow(output)

    def compose_output_row(self, row):
        output = {}
        output["measurement_id"] = ""
        output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
        output["measurement_concept_id"] = row['c.name'].omop_id
        output["measurement_date"] = self.transform_float_to_date(row['c.date'].biobank_value)
        output["measurement_datetime"] = ""
        output["measurement_time"] = ""
        output["measurement_type_concept_id"] = ""
        output["operator_concept_id"] = ""
        output["value_as_number"] = ""
        output["value_as_concept_id"] = ""
        output["unit_concept_id"] = ""
        output["range_low"] = ""
        output["range_high"] = ""
        output["provider_id"] = ""
        output["visit_occurrence_id"] = ""
        output["visit_detail_id"] = ""
        output["measurement_source_value"] = row['c.value'].biobank_value
        output["measurement_source_concept_id"] = ""
        output["unit_source_value"] = ""
        output["value_source_value"] = ""

        return output