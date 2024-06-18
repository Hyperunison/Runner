from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField
from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv

from src.Service.Workflows.PersonIdGenerator import PersonIdGenerator


class OMOPoficationDrugExposure(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, UCDMConvertedField]], person_id_generator: PersonIdGenerator):
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
                output["person_id"] = person_id_generator.get_person_id_int(row['participant_id'].biobank_value)
                output["drug_concept_id"] = self.render_omop_id(row, 'c.name')
                output["drug_exposure_start_date"] = self.render_ucdm_value(row, 'c.start_date')
                output["drug_exposure_start_datetime"] = self.render_ucdm_value(row, 'c.start_datetime')
                output["drug_exposure_end_date"] = self.render_ucdm_value(row, 'c.end_date')
                output["drug_exposure_end_datetime"] = self.render_ucdm_value(row, 'c.end_datetime')
                output["verbatim_end_date"] = self.render_ucdm_value(row, 'c.verbatim_end_date')
                output["drug_type_concept_id"] = self.render_omop_id(row, 'c.type')
                output["stop_reason"] = self.render_ucdm_value(row, 'c.stop_reason')
                output["refills"] = self.render_ucdm_value(row, 'c.refills')
                output["quantity"] = self.render_ucdm_value(row, 'c.quantity')
                output["days_supply"] = self.render_ucdm_value(row, 'c.days_supply')
                output["sig"] = self.render_ucdm_value(row, 'c.sig')
                output["route_concept_id"] = self.render_omop_id(row, 'c.route')
                output["lot_number"] = self.render_ucdm_value(row, 'c.lot_number')
                output["provider_id"] = self.render_ucdm_value(row, 'c.provider')
                output["visit_occurrence_id"] = self.render_ucdm_value(row, 'c.visit_occurrence')
                output["visit_detail_id"] = self.render_ucdm_value(row, 'c.visit_detail')
                output["drug_source_value"] = self.render_biobank_value(row, 'c.name')
                output["drug_source_concept_id"] = ""
                output["route_source_value"] = self.render_biobank_value(row, 'c.route')
                output["dose_unit_source_value"] = ""
                num += 1
                writer.writerow(output)