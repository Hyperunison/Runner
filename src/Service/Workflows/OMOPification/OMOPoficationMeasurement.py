from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationMeasurement(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, str]]):
        header = ["measurement_id", "person_id", "measurement_concept_id", "measurement_date",
                  "measurement_datetime", "measurement_time", "measurement_type_concept_id",
                  "operator_concept_id", "value_as_number", "value_as_concept_id",
                  "unit_concept_id", "range_low", "range_high", "provider_id", "visit_occurrence_id",
                  "visit_detail_id", "measurement_source_value", "measurement_source_concept_id",
                  "unit_source_value", "value_source_value"]
        filename = self.dir + "/measurement.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["measurement_id"] = ""
                output["person_id"] = row['participant_id'].biobank_value
                output["measurement_concept_id"] = ""
                output["measurement_date"] = ""
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
                output["measurement_source_value"] = ""
                output["measurement_source_concept_id"] = ""
                output["unit_source_value"] = ""
                output["value_source_value"] = ""
                writer.writerow(output)