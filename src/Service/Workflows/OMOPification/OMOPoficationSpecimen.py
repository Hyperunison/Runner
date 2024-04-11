from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationSpecimen(OMOPoficationBase):
    def build(self, ucdm: List[Dict[str, str]]):
        header = ["specimen_id", "person_id", "specimen_concept_id", "specimen_type_concept_id",
                  "specimen_date", "specimen_datetime", "quantity", "unit_concept_id",
                  "anatomic_site_concept_id", "disease_status_concept_id", "specimen_source_id",
                  "specimen_source_value", "unit_source_value", "anatomic_site_source_value",
                  "disease_status_source_value"]
        filename = self.dir + "/specimen.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                output["specimen_id"] = ""
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["specimen_concept_id"] = ""
                output["specimen_type_concept_id"] = ""
                output["specimen_date"] = ""
                output["specimen_datetime"] = ""
                output["quantity"] = ""
                output["unit_concept_id"] = ""
                output["anatomic_site_concept_id"] = ""
                output["disease_status_concept_id"] = ""
                output["specimen_source_id"] = ""
                output["specimen_source_value"] = ""
                output["unit_source_value"] = ""
                output["anatomic_site_source_value"] = ""
                output["disease_status_source_value"] = ""
                writer.writerow(output)