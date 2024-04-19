from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationDrugExpose(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, str]]):
        header = ["drug_exposure_id", "person_id", "drug_concept_id", "drug_exposure_start_date",
                  "drug_exposure_start_datetime", "drug_exposure_end_date", "drug_exposure_end_datetime",
                  "verbatim_end_date", "drug_type_concept_id", "stop_reason", "refills",
                  "quantity", "days_supply", "sig", "route_concept_id", "lot_number", "provider_id",
                  "visit_occurrence_id", "visit_detail_id", "drug_source_value", "drug_source_concept_id",
                  "route_source_value", "dose_unit_source_value"]
        filename = self.dir + "/drug_exposure.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            num: int = 1
            for row in ucdm:
                output = {}
                output["drug_exposure_id"] = str(num)
                output["person_id"] = self.transform_person_id_to_integer(row['participant_id'].biobank_value)
                output["drug_concept_id"] = row['c.name'].ucdm_value if 'c.name' in row else ''
                output["drug_exposure_start_date"] = ""
                output["drug_exposure_start_datetime"] = ""
                output["drug_exposure_end_date"] = ""
                output["drug_exposure_end_datetime"] = ""
                output["verbatim_end_date"] = ""
                output["drug_type_concept_id"] = ""
                output["stop_reason"] = row['c.stop_reason'].ucdm_value if 'c.stop_reason' in row else ''
                output["refills"] = ""
                output["quantity"] = row['c.quantity'].ucdm_value if 'c.quantity' in row else ''
                output["days_supply"] = ""
                output["sig"] = ""
                output["route_concept_id"] = row['c.route'].ucdm_value if 'c.route' in row else ''
                output["lot_number"] = ""
                output["provider_id"] = ""
                output["visit_occurrence_id"] = ""
                output["visit_detail_id"] = ""
                output["drug_source_value"] = row['c.name'].biobank_value if 'c.name' in row else ''
                output["drug_source_concept_id"] = ""
                output["route_source_value"] = row['c.route'].biobank_value if 'c.route' in row else ''
                output["dose_unit_source_value"] = ""
                num += 1
                writer.writerow(output)