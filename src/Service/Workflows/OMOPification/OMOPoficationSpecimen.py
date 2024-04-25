from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationSpecimen(OMOPoficationBase):
    def build(self, ucdm: List[Dict[str, UCDMConvertedField]]):
        header = ["specimen_id", "person_id", "specimen_concept_id", "specimen_type_concept_id",
                  "specimen_date", "specimen_datetime", "quantity", "unit_concept_id",
                  "anatomic_site_concept_id", "disease_status_concept_id", "specimen_source_id",
                  "specimen_source_value", "unit_source_value", "anatomic_site_source_value",
                  "disease_status_source_value"]
        filename = self.dir + "/specimen.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            num: int = 1
            for row in ucdm:
                output = {}
                output["specimen_id"] = str(num)
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["specimen_concept_id"] = row['c.name'].omop_id if 'c.name' in row else ''
                output["specimen_type_concept_id"] = row['c.type'].omop_id if 'c.type' in row else ''
                output["specimen_date"] = row['c.date'].ucdm_value if 'c.date' in row else ''
                output["specimen_datetime"] = row['c.datetime'].ucdm_value if 'c.datetime' in row else ''
                output["quantity"] = row['c.quantity'].ucdm_value if 'c.quantity' in row else ''
                output["unit_concept_id"] = row['c.unit'].omop_id if 'c.unit' in row else ''
                output["anatomic_site_concept_id"] = row['c.anatomic_site'].omop_id if 'c.anatomic_site' in row else ''
                output["disease_status_concept_id"] = row['c.disease_status'].omop_id if 'c.disease_status' in row else ''
                output["specimen_source_id"] = ""
                output["specimen_source_value"] = row['c.name'].biobank_value if 'c.name' in row else ''
                output["unit_source_value"] = row['c.unit'].biobank_value if 'c.unit' in row else ''
                output["anatomic_site_source_value"] = row['c.anatomic_site'].biobank_value if 'c.anatomic_site' in row else ''
                output["disease_status_source_value"] = row['c.disease_status'].biobank_value if 'c.disease_status' in row else ''
                num += 1
                writer.writerow(output)