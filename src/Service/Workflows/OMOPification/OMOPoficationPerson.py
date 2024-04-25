from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationPerson(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, str]]):
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
                output["gender_concept_id"] = row['gender'].omop_id if 'gender' in row else ''
                output["year_of_birth"] = row['year_of_birth'].ucdm_value if 'year_of_birth' in row else ''
                output["month_of_birth"] = ""                               # todo: calculate
                output["day_of_birth"] = ""                                 # todo: calculate
                output["birth_datetime"] = ""                               # todo: calculate
                output["race_concept_id"] = row['race'].omop_id if 'race' in row else ''
                output["ethnicity_concept_id"] = row['ethnicity'].omop_id if 'ethnicity' in row else ''
                output["location_id"] = row['c.location'].omop_id if 'c.location' in row else ''
                output["provider_id"] = row['c.provider'].omop_id if 'c.provider' in row else ''
                output["care_site_id"] = row['c.care_site'].omop_id if 'c.care_site' in row else ''
                output["person_source_value"] = row['participant_id'].biobank_value if 'race' in row else ''
                output["gender_source_value"] = row['gender'].ucdm_value if 'gender' in row else ''
                output["gender_source_concept_id"] = ""
                output["race_source_value"] = row['race'].ucdm_value if 'race' in row else ''
                output["race_source_concept_id"] = ""
                output["ethnicity_source_value"] = row['ethnicity'].ucdm_value if 'ethnicity' in row else ''
                output["ethnicity_source_concept_id"] = ""
                writer.writerow(output)