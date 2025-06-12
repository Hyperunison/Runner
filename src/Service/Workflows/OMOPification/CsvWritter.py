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
    ) -> Dict[str, int]:
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            header = [item['name'] for item in fields_map.values()]
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()
            skip_rows: Dict[str, int] = {}
            for row in ucdm:
                output = {}
                skip_reasons: Dict[str, int] = self.lines_filter.get_line_errors(row, fields_map)

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
                    for reason, count in skip_reasons.items():
                        if not reason in skip_rows:
                            skip_rows[reason] = 0
                        skip_rows[reason] += count

            return skip_rows
