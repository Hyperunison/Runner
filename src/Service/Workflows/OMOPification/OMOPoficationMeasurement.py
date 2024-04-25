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
                output["measurement_date"] = row['c.date'].biobank_value if 'c.date' in row else ''
                output["measurement_datetime"] = row['c.datetime'].ucdm_value if 'c.datetime' in row else ''
                output["measurement_time"] = ""
                output["measurement_type_concept_id"] = row['c.type'].omop_id if 'c.type' in row else ''
                output["operator_concept_id"] = row['c.operator'].omop_id if 'c.operator' in row else ''
                output["value_as_number"] = row['c.value_as_number'].ucdm_value if 'c.value_as_number' in row else ''
                output["value_as_concept_id"] = row['c.name'].omop_id if 'c.name' in row else ''
                output["unit_concept_id"] = row['c.unit'].omop_id if 'c.unit' in row else ''
                output["range_low"] = row['c.range_low'].ucdm_value if 'c.range_low' in row else ''
                output["range_high"] = row['c.range_high'].ucdm_value if 'c.range_high' in row else ''
                output["provider_id"] = row['c.provider'].omop_id if 'c.provider' in row else ''
                output["visit_occurrence_id"] = row['c.visit_occurrence'].ucdm_value if 'c.visit_occurrence' in row else ''
                output["visit_detail_id"] = row['c.visit_detail'].ucdm_value if 'c.visit_detail' in row else ''
                output["measurement_source_value"] = row['c.value'].biobank_value if 'c.value' in row else ''
                output["measurement_source_concept_id"] = ""
                output["unit_source_value"] = row['c.unit'].biobank_value if 'c.unit' in row else ''
                output["value_source_value"] = row['c.value'].biobank_value if 'c.value' in row else ''
                num += 1
                writer.writerow(output)