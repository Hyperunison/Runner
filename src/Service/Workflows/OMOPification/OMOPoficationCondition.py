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
                output["condition_concept_id"] = row['c.name'].omop_id if 'c.name' in row else ''
                output["condition_start_date"] = row['c.start_date'].ucdm_value if 'c.start_date' in row else ''
                output["condition_start_datetime"] = row['c.start_datetime'].ucdm_value if 'c.start_datetime' in row else ''
                output["condition_end_date"] = row['c.end_date'].ucdm_value if 'c.end_date' in row else ''
                output["condition_end_datetime"] = row['c.end_datetime'].ucdm_value if 'c.end_datetime' in row else ''
                output["condition_type_concept_id"] = row['c.type'].omop_id if 'c.type' in row else ''
                output["condition_status_concept_id"] = row['c.status'].omop_id if 'c.status' in row else ''
                output["stop_reason"] = row['c.stop_reason'].ucdm_value if 'c.stop_reason' in row else ''
                output["provider_id"] = row['c.provider'].omop_id if 'c.provider' in row else ''
                output["visit_occurrence_id"] = row['c.visit_occurrence'].ucdm_value if 'c.visit_occurrence' in row else ''
                output["visit_detail_id"] = row['c.visit_detail'].ucdm_value if 'c.visit_detail' in row else ''
                output["condition_source_value"] = row['c.name'].biobank_value if 'c.name' in row else ''
                output["condition_source_concept_id"] = ""
                output["condition_status_source_value"] = row['c.status'].biobank_value if 'c.status' in row else ''
                num += 1

                writer.writerow(output)