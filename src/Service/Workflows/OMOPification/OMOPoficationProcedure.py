from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationProcedure(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, str]]):
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
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["procedure_concept_id"] = row['c.name'].omop_id if 'c.name' in row else ''
                output["procedure_date"] = row['c.date'].biobank_value if 'c.date' in row else ''
                output["procedure_datetime"] = row['c.datetime'].biobank_value if 'c.datetime' in row else ''
                output["procedure_type_concept_id"] = row['c.type'].omop_id if 'c.type' in row else ''
                output["modifier_concept_id"] = row['c.modifier'].omop_id if 'c.modifier' in row else ''
                output["quantity"] = row['c.quantity'].ucdm_value if 'c.quantity' in row else ''
                output["provider_id"] = row['c.provider'].omop_id if 'c.provider' in row else ''
                output["visit_occurrence_id"] = row['c.visit_occurrence'].ucdm_value if 'c.visit_occurrence' in row else ''
                output["visit_detail_id"] = row['c.visit_detail'].ucdm_value if 'c.visit_detail' in row else ''
                output["procedure_source_value"] = row['c.value'].biobank_value if 'c.value' in row else ''
                output["procedure_source_concept_id"] = ""
                output["modifier_source_value"] = row['c.modifier'].biobank_value if 'c.modifier' in row else ''
                num += 1
                writer.writerow(output)