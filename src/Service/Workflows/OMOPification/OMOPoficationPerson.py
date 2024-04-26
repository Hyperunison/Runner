from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationPerson(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, UCDMConvertedField]]):
        header = ["person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime",
                  "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id",
                  "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value",
                  "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"]
        filename = self.dir + "/person.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["gender_concept_id"] = self.render_omop_id(row, 'gender')
                output["year_of_birth"] = self.render_ucdm_value(row, 'year_of_birth')
                output["month_of_birth"] = ""                               # todo: calculate
                output["day_of_birth"] = ""                                 # todo: calculate
                output["birth_datetime"] = ""                               # todo: calculate
                output["race_concept_id"] = self.render_omop_id(row, 'race')
                output["ethnicity_concept_id"] = self.render_omop_id(row, 'ethnicity')
                output["location_id"] = self.render_omop_id(row, 'c.location')
                output["provider_id"] = self.render_omop_id(row, 'c.provider')
                output["care_site_id"] = self.render_omop_id(row, 'c.care_site')
                output["person_source_value"] = self.render_biobank_value(row, 'participant_id')
                output["gender_source_value"] = self.render_ucdm_value(row, 'gender')
                output["gender_source_concept_id"] = ""
                output["race_source_value"] = self.render_ucdm_value(row, 'race')
                output["race_source_concept_id"] = ""
                output["ethnicity_source_value"] = self.render_ucdm_value(row, 'ethnicity')
                output["ethnicity_source_concept_id"] = ""
                writer.writerow(output)