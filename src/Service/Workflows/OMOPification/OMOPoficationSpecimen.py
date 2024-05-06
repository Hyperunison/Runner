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
                output["specimen_concept_id"] = self.render_omop_id(row, 'c.name')
                output["specimen_type_concept_id"] = self.render_omop_id(row, 'c.type')
                output["specimen_date"] = self.render_ucdm_value(row, 'c.date')
                output["specimen_datetime"] = self.render_ucdm_value(row, 'c.datetime')
                output["quantity"] = self.render_ucdm_value(row, 'c.quantity')
                output["unit_concept_id"] = self.render_omop_id(row, 'c.unit')
                output["anatomic_site_concept_id"] = self.render_omop_id(row, 'c.anatomic_site')
                output["disease_status_concept_id"] = self.render_omop_id(row, 'c.disease_status')
                output["specimen_source_id"] = ""
                output["specimen_source_value"] = self.render_biobank_value(row, 'c.name')
                output["unit_source_value"] = self.render_biobank_value(row, 'c.unit')
                output["anatomic_site_source_value"] = self.render_biobank_value(row, 'c.anatomic_site')
                output["disease_status_source_value"] = self.render_biobank_value(row, 'c.disease_status')
                num += 1
                writer.writerow(output)