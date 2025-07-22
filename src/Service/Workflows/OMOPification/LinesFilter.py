from typing import List, Dict

import yaml

from src.Service.UCDMConvertedField import UCDMConvertedField

class LinesFilter:
    def get_line_errors(
            self,
            row: Dict[str, UCDMConvertedField],
            fields_map: Dict[str, Dict[str, str]]
    ) -> Dict[str, int]:
        skip_reasons: Dict[str, int] = {}
        for key, val in row.items():
            value = val.export_value
            if (value == '' or value is None or value == '0' or value == 0) and fields_map[key]['isRequired']:
                reason = "{} is empty, but required (value={})".format(fields_map[key]['name'], value)
                if not reason in skip_reasons:
                    skip_reasons[reason] = 0
                skip_reasons[reason] += 1

        return skip_reasons