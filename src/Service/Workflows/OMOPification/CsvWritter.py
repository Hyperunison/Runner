from typing import List, Dict
import csv
from src.Service.UCDMConvertedField import UCDMConvertedField

class CsvWritter:

    def build(
            self,
            filename: str,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        with open(filename, 'w', newline='') as file:
            header = [item['name'] for item in fields_map.values()]
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()
            skip_rows: List[str] = []
            for row in ucdm:
                output = {}
                skip_reasons: List[str] = []
                for key, val in row.items():
                    value = val.export_value
                    if (value == '' or value is None or value == '0' or value == 0) and fields_map[key]['isRequired']:
                        skip_reasons.append("{} is empty, but required (value={})".format(key, value))
                    field_name = fields_map[key]['name']
                    output[field_name] = value if value is not None else ''

                if len(skip_reasons) == 0:
                    writer.writerow(output)
                else:
                    row_str: Dict[str, str] = {}
                    for k, v in row.items():
                        row_str[k] = v.export_value
                    skip_rows.append("Skip row as [{}]. Row={}".format(", ".join(skip_reasons), str(row_str)))

            return skip_rows