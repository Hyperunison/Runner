from typing import List, Dict
from src.Service.UCDMConvertedField import UCDMConvertedField

class LinesFilter:

    def get_line_errors(
            self,
            row: Dict[str, UCDMConvertedField],
            fields_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        skip_reasons: List[str] = []
        for key, val in row.items():
            value = val.export_value
            if (value == '' or value is None or value == '0' or value == 0) and fields_map[key]['isRequired']:
                skip_reasons.append("{} is empty, but required (value={})".format(key, value))

        return skip_reasons