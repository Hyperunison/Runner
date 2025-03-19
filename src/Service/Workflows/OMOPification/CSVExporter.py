from typing import List, Dict, Optional

from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.Workflows.OMOPification.BaseDatabaseExporter import BaseDatabaseExporter
from src.Service.Workflows.OMOPification.CsvWritter import CsvWritter


class CSVExporter(BaseDatabaseExporter):
    dir: str = "var/"

    def __init__(self):
        super().__init__()

    def save_rows(
            self,
            table_name: str,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]],
            columns: List[Dict[str, str]]
    ) -> List[str]:
        csv_writter = CsvWritter()

        skipped_rows = csv_writter.build(self.get_export_table_filename(table_name), ucdm, fields_map)

        return skipped_rows

    def write_single_table_dump(self, table_name: str) -> Optional[str]:
        return self.get_export_table_filename(table_name)

    def get_export_table_filename(self, table_name: str) -> Optional[str]:
        return self.dir + "{}.csv".format(table_name)
