from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationDrugExposure(OMOPoficationBase):

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
                output["drug_exposure_start_date"] = row['c.start_date'].ucdm_value if 'c.start_date' in row else ''
                output["drug_exposure_start_datetime"] = row['c.start_datetime'].ucdm_value if 'c.start_datetime' in row else ''
                output["drug_exposure_end_date"] = row['c.end_date'].ucdm_value if 'c.end_date' in row else ''
                output["drug_exposure_end_datetime"] = row['c.end_datetime'].ucdm_value if 'c.end_datetime' in row else ''
                output["verbatim_end_date"] = row['c.verbatim_end_date'].ucdm_value if 'c.verbatim_end_date' in row else ''
                output["drug_type_concept_id"] = row['c.type'].omop_id if 'c.type' in row else ''
                output["stop_reason"] = row['c.stop_reason'].ucdm_value if 'c.stop_reason' in row else ''
                output["refills"] = row['c.refills'].ucdm_value if 'c.refills' in row else ''
                output["quantity"] = row['c.quantity'].ucdm_value if 'c.quantity' in row else ''
                output["days_supply"] = row['c.days_supply'].ucdm_value if 'c.days_supply' in row else ''
                output["sig"] = row['c.sig'].ucdm_value if 'c.sig' in row else ''
                output["route_concept_id"] = row['c.route'].ucdm_value if 'c.route' in row else ''
                output["lot_number"] = row['c.lot_number'].ucdm_value if 'c.lot_number' in row else ''
                output["provider_id"] = row['c.provider'].omop_id if 'c.provider' in row else ''
                output["visit_occurrence_id"] = row['c.visit_occurrence'].ucdm_value if 'c.visit_occurrence' in row else ''
                output["visit_detail_id"] = row['c.visit_detail'].ucdm_value if 'c.visit_detail' in row else ''
                output["drug_source_value"] = row['c.name'].biobank_value if 'c.name' in row else ''
                output["drug_source_concept_id"] = ""
                output["route_source_value"] = row['c.route'].biobank_value if 'c.route' in row else ''
                output["dose_unit_source_value"] = ""
                num += 1
                writer.writerow(output)