from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationVisitOccurrence(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, UCDMConvertedField]]):
        header = ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date",
                  "visit_start_datetime", "visit_end_date", "visit_end_datetime", "visit_type_concept_id",
                  "provider_id", "care_site_id", "visit_source_value", "visit_source_concept_id",
                  "admitting_source_concept_id", "admitting_source_value", "discharge_to_concept_id",
                  "discharge_to_source_value", "preceding_visit_occurrence_id"]
        filename = self.dir + "/visit_occurrence.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["visit_occurrence_id"] = ""
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["visit_concept_id"] = row['c.name'].omop_id if 'c.name' in row else ''
                output["visit_start_date"] = row['c.start_date'].ucdm_value if 'c.start_date' in row else ''
                output["visit_start_datetime"] = row['c.start_datetime'].ucdm_value if 'c.start_datetime' in row else ''
                output["visit_end_date"] = row['c.end_date'].ucdm_value if 'c.end_date' in row else ''
                output["visit_end_datetime"] = row['c.end_datetime'].ucdm_value if 'c.end_datetime' in row else ''
                output["visit_type_concept_id"] = row['c.type'].omop_id if 'c.type' in row else ''
                output["provider_id"] = row['c.provider'].omop_id if 'c.provider' in row else ''
                output["care_site_id"] = row['c.care_site'].omop_id if 'c.care_site' in row else ''
                output["visit_source_value"] = row['c.name'].biobank_value if 'c.name' in row else ''
                output["visit_source_concept_id"] = ""
                output["admitting_source_concept_id"] = ''
                output["admitting_source_value"] = row['c.admitting'].ucdm_value if 'c.admitting' in row else ''
                output["discharge_to_concept_id"] = ''
                output["discharge_to_source_value"] = row['c.discharge'].ucdm_value if 'c.discharge' in row else ''
                output["preceding_visit_occurrence_id"] = row['c.preceding_visit_occurrence_id'].ucdm_value if 'c.preceding_visit_occurrence_id' in row else ''
                writer.writerow(output)