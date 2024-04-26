from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationCondition(OMOPoficationBase):
    def build(self, ucdm: List[Dict[str, UCDMConvertedField]]):
        header = ["condition_occurrence_id", "person_id", "condition_concept_id", "condition_start_date",
                  "condition_start_datetime", "condition_end_date", "condition_end_datetime",
                  "condition_type_concept_id",
                  "condition_status_concept_id", "stop_reason", "provider_id", "visit_occurrence_id", "visit_detail_id",
                  "condition_source_value", "condition_source_concept_id", "condition_status_source_value"]
        filename = self.dir + "/condition_occurrence.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            num: int = 1
            for row in ucdm:
                output = {}
                output["condition_occurrence_id"] = str(num)
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["condition_concept_id"] = self.render_omop_id(row, 'c.name')
                output["condition_start_date"] = self.render_ucdm_value(row, 'c.start_date')
                output["condition_start_datetime"] = self.render_ucdm_value(row, 'c.start_datetime')
                output["condition_end_date"] = self.render_ucdm_value(row, 'c.end_date')
                output["condition_end_datetime"] = self.render_ucdm_value(row, 'c.end_datetime')
                output["condition_type_concept_id"] = self.render_omop_id(row, 'c.type')
                output["condition_status_concept_id"] = self.render_omop_id(row, 'c.status')
                output["stop_reason"] = self.render_ucdm_value(row, 'c.stop_reason')
                output["provider_id"] = self.render_omop_id(row, 'c.provider')
                output["visit_occurrence_id"] = self.render_ucdm_value(row, 'c.visit_occurrence')
                output["visit_detail_id"] = self.render_ucdm_value(row, 'c.visit_detail')
                output["condition_source_value"] = self.render_biobank_value(row, 'c.name')
                output["condition_source_concept_id"] = ""
                output["condition_status_source_value"] = self.render_biobank_value(row, 'c.status')
                num += 1

                writer.writerow(output)