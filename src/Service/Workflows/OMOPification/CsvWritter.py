from typing import List, Dict
import csv
from src.Service.UCDMConvertedField import UCDMConvertedField
from src.Service.Workflows.OMOPification.LinesFilter import LinesFilter


class CsvWritter:
    lines_filter: LinesFilter

    def __init__(self):
        self.lines_filter = LinesFilter()

    def build(
            self,
            filename: str,
            ucdm: List[Dict[str, UCDMConvertedField]],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            header = [item['name'] for item in fields_map.values()]
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()
            skip_rows: List[str] = []
            for row in ucdm:
                output = {}
                skip_reasons: List[str] = self.lines_filter.get_line_errors(row, fields_map)

                if len(skip_reasons) == 0:
                    for key, val in row.items():
                        if key == 'participant_id' and key not in fields_map:
                            continue
                        if key.endswith('.__bridge_id'):
                            continue
                        value = val.export_value
                        field_name = fields_map[key]['name']
                        output[field_name] = value if value is not None else ''

                    writer.writerow(output)
                else:
                    row_str: Dict[str, str] = {}
                    for k, v in row.items():
                        row_str[k] = v.export_value
                    skip_rows.append("Skip row as [{}]. Row={}".format(", ".join(skip_reasons), str(row_str)))

            return skip_rows
