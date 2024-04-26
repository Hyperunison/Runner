from datetime import datetime
from typing import List, Dict

from src.Service.UCDMResolver import UCDMConvertedField


class OMOPoficationBase:
    dir: str = "var/"

    def transform_person_id_to_integer(self, origin_value):
        return int(float(self.clear_person_id(origin_value)))

    def get_dir(self):
        return self.dir

    def clear_person_id(self, origin_value):
        return origin_value.replace("DKFZ-I00", "")

    def render_omop_id(self, row: Dict[str, UCDMConvertedField],  field: str) -> str:
        return row[field].omop_id if field in row and row[field] is not None else ''
    def render_ucdm_value(self, row: Dict[str, UCDMConvertedField], field: str) -> str:
        return row[field].ucdm_value if field in row and row[field] is not None else ''
    def render_biobank_value(self, row: Dict[str, UCDMConvertedField], field: str) -> str:
        return row[field].biobank_value if field in row and row[field] is not None else ''
    def render_date(self, row: Dict[str, UCDMConvertedField], field: str) -> str:
        return row[field].biobank_value if (field in row and row[field] is not None) else ''
    def render_datetime(self, row: Dict[str, UCDMConvertedField], field: str) -> str:
        return row[field].biobank_value if (field in row and row[field] is not None) else ''
