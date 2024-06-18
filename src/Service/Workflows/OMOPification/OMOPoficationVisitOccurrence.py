from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv

from src.Service.Workflows.PersonIdGenerator import PersonIdGenerator


class OMOPoficationVisitOccurrence(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, UCDMConvertedField]], person_id_generator: PersonIdGenerator):
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
                output["person_id"] = person_id_generator.get_person_id_int(row['participant_id'].biobank_value)
                output["visit_concept_id"] = self.render_omop_id(row, 'c.name')
                output["visit_start_date"] = self.render_ucdm_value(row, 'c.start_date')
                output["visit_start_datetime"] = self.render_ucdm_value(row, 'c.start_datetime')
                output["visit_end_date"] = self.render_ucdm_value(row, 'c.end_date')
                output["visit_end_datetime"] = self.render_ucdm_value(row, 'c.end_datetime')
                output["visit_type_concept_id"] = self.render_omop_id(row, 'c.type')
                output["provider_id"] = self.render_omop_id(row, 'c.provider')
                output["care_site_id"] = self.render_omop_id(row, 'c.care_site')
                output["visit_source_value"] = self.render_biobank_value(row, 'c.name')
                output["visit_source_concept_id"] = ""
                output["admitting_source_concept_id"] = ''
                output["admitting_source_value"] = self.render_ucdm_value(row, 'c.admitting')
                output["discharge_to_concept_id"] = ''
                output["discharge_to_source_value"] = self.render_ucdm_value(row, 'c.discharge')
                output["preceding_visit_occurrence_id"] = self.render_ucdm_value(row, 'c.preceding_visit_occurrence_id')
                writer.writerow(output)