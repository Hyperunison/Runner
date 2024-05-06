from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationMeasurement(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, UCDMConvertedField]]):
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
                output["measurement_concept_id"] = self.render_omop_id(row, 'c.name')
                output["measurement_date"] = self.render_date(row, 'c.date')
                output["measurement_datetime"] = self.render_ucdm_value(row, 'c.datetime')
                output["measurement_time"] = ""
                output["measurement_type_concept_id"] = self.render_omop_id(row, 'c.type')
                output["operator_concept_id"] = self.render_omop_id(row, 'c.operator')
                output["value_as_number"] = self.render_ucdm_value(row, 'c.value_as_number')
                output["value_as_concept_id"] = self.render_omop_id(row, 'c.value')
                output["unit_concept_id"] = self.render_omop_id(row, 'c.unit')
                output["range_low"] = self.render_ucdm_value(row, 'c.range_low')
                output["range_high"] = self.render_ucdm_value(row, 'c.range_high')
                output["provider_id"] = self.render_omop_id(row, 'c.provider')
                output["visit_occurrence_id"] = self.render_ucdm_value(row, 'c.visit_occurrence')
                output["visit_detail_id"] = self.render_ucdm_value(row, 'c.visit_detail')
                output["measurement_source_value"] = self.render_biobank_value(row, 'c.value')
                output["measurement_source_concept_id"] = ""
                output["unit_source_value"] = self.render_biobank_value(row, 'c.unit')
                output["value_source_value"] = self.render_biobank_value(row, 'c.value')
                num += 1
                writer.writerow(output)