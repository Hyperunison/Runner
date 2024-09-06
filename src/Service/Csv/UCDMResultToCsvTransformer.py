from typing import List, Dict, Tuple
import csv
from io import StringIO

from src.Service.UCDMConvertedField import UCDMConvertedField

class UCDMResultToCsvTransformer:

    def transform(self, result: List[Dict[str, UCDMConvertedField]]) -> str:
        header = self.build_header(result[0])
        rows = self.build_rows(header, result)
        output = StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=header)
        csv_writer.writeheader()
        csv_writer.writerows(rows)
        csv_content = output.getvalue()
        output.close()

        return csv_content

    def build_header(self, row: Dict[str, UCDMConvertedField]) -> List[str]:
        return list(row.keys())

    def build_rows(self, header: List[str], rows: List[Dict[str, UCDMConvertedField]]) -> List[Dict[str, str]]:
        result = []

        for row in rows:
            result_item = {}

            for name in header:
                result_item[name] = row[name].export_value

            result.append(result_item)

        return result