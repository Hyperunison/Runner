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
                output["person_id"] = row['participant_id'].biobank_value
                output["gender_concept_id"] = row['gender'].omop_id
                output["year_of_birth"] = row['year_of_birth'].ucdm_value
                output["month_of_birth"] = ""                               # todo: calculate
                output["day_of_birth"] = ""                                 # todo: calculate
                output["birth_datetime"] = ""                               # todo: calculate
                output["race_concept_id"] = row['race'].omop_id
                output["ethnicity_concept_id"] = row['ethnicity'].omop_id
                output["location_id"] = ""                                  # todo
                output["provider_id"] = ""                                  # todo
                output["care_site_id"] = ""                                 # todo
                output["person_source_value"] = row['participant_id'].biobank_value
                output["gender_source_value"] = row['gender'].ucdm_value
                output["gender_source_concept_id"] = ""
                output["race_source_value"] = row['race'].ucdm_value
                output["race_source_concept_id"] = ""
                output["ethnicity_source_value"] = row['ethnicity'].ucdm_value
                output["ethnicity_source_concept_id"] = ""
                writer.writerow(output)