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
            num: int = 1
            for row in ucdm:
                output = {}
                output["measurement_id"] = num
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["measurement_concept_id"] = row['c.name'].omop_id if 'c.name' in row else ''
                output["measurement_date"] = self.transform_float_to_date(row['c.date'].biobank_value) if 'c.date' in row else ''
                output["measurement_datetime"] = ""
                output["measurement_time"] = ""
                output["measurement_type_concept_id"] = ""
                output["operator_concept_id"] = ""
                output["value_as_number"] = row['c.value_as_number'].ucdm_value if 'c.value_as_number' in row else ''
                output["value_as_concept_id"] = row['c.name'].omop_id if 'c.name' in row else ''
                output["unit_concept_id"] = row['c.unit'].omop_id if 'c.unit' in row else ''
                output["range_low"] = ""
                output["range_high"] = ""
                output["provider_id"] = ""
                output["visit_occurrence_id"] = ""
                output["visit_detail_id"] = ""
                output["measurement_source_value"] = row['c.value'].biobank_value if 'c.value' in row else ''
                output["measurement_source_concept_id"] = ""
                output["unit_source_value"] = row['c.unit'].biobank_value if 'c.unit' in row else ''
                output["value_source_value"] = row['c.value'].biobank_value if 'c.value' in row else ''
                num += 1
                writer.writerow(output)