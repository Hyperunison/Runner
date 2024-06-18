from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv

from src.Service.Workflows.PersonIdGenerator import PersonIdGenerator


class OMOPoficationPerson(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, UCDMConvertedField]], person_id_generator: PersonIdGenerator):
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
                output["person_id"] = person_id_generator.get_person_id_int(row['participant_id'].biobank_value)
                output["gender_concept_id"] = self.render_omop_id(row, 'gender')
                output["year_of_birth"] = self.render_ucdm_value(row, 'year_of_birth')
                output["month_of_birth"] = self.render_ucdm_value(row, 'month_of_birth')
                output["day_of_birth"] = self.render_ucdm_value(row, 'day_of_birth')
                output["birth_datetime"] = self.render_ucdm_value(row, 'birth_datetime')
                output["race_concept_id"] = self.render_omop_id(row, 'race')
                output["ethnicity_concept_id"] = self.render_ucdm_value(row, 'ethnicity')
                output["location_id"] = self.render_ucdm_value(row, 'c.location_id')
                output["provider_id"] = self.render_ucdm_value(row, 'c.provider_id')
                output["care_site_id"] = self.render_ucdm_value(row, 'c.care_site_id')
                output["person_source_value"] = self.render_biobank_value(row, 'participant_id')
                output["gender_source_value"] = self.render_ucdm_value(row, 'gender')
                output["gender_source_concept_id"] = self.render_omop_id(row, 'c.gender_source')
                output["race_source_value"] = self.render_ucdm_value(row, 'race')
                output["race_source_concept_id"] = self.render_omop_id(row, 'c.race_source')
                output["ethnicity_source_value"] = self.render_ucdm_value(row, 'ethnicity')
                output["ethnicity_source_concept_id"] = self.render_omop_id(row, 'c.ethnicity_source')
                writer.writerow(output)