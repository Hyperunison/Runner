from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv

from src.Service.Workflows.PersonIdGenerator import PersonIdGenerator


class OMOPoficationProcedure(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, UCDMConvertedField]], person_id_generator: PersonIdGenerator):
        header = ["procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date",
                  "procedure_datetime", "procedure_type_concept_id", "modifier_concept_id",
                  "quantity", "provider_id", "visit_occurrence_id", "visit_detail_id",
                  "procedure_source_value", "procedure_source_concept_id", "modifier_source_value"]
        filename = self.dir + "/procedure.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            num: int = 1
            for row in ucdm:
                output = {}
                output["procedure_occurrence_id"] = str(num)
                output["person_id"] = person_id_generator.get_person_id_int(row['participant_id'].biobank_value)
                output["procedure_concept_id"] = self.render_omop_id(row, 'c.name')
                output["procedure_date"] = self.render_biobank_value(row, 'c.date')
                output["procedure_datetime"] = self.render_biobank_value(row, 'c.datetime')
                output["procedure_type_concept_id"] = self.render_omop_id(row, 'c.type')
                output["modifier_concept_id"] = self.render_omop_id(row, 'c.modifier')
                output["quantity"] = self.render_ucdm_value(row, 'c.quantity')
                output["provider_id"] =self.render_omop_id(row, 'c.provider')
                output["visit_occurrence_id"] = self.render_ucdm_value(row, 'c.visit_occurrence')
                output["visit_detail_id"] = self.render_ucdm_value(row, 'c.visit_detail')
                output["procedure_source_value"] = self.render_biobank_value(row, 'c.value')
                output["procedure_source_concept_id"] = ""
                output["modifier_source_value"] = self.render_biobank_value(row, 'c.modifier')
                num += 1
                writer.writerow(output)