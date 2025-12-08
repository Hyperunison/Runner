from typing import List, Dict, Tuple
from src.Api import Api


class UCDMMappingResolver:
    mapping: List

    def __init__(self, mapping: List):
        self.mapping = mapping

    def resolve(self) -> Dict[str, Dict[str, List[Tuple[str, str, str]]]]:
        if self.mapping is None:
            self.load_mapping_via_api()

        return self.transform_mapping_to_resolve_result()

    def load_mapping_via_api(self):
        pass

    def transform_mapping_to_resolve_result(self) -> Dict[str, Dict[str, Dict[str, List[Tuple[str, str, str]]]]]:
        index: Dict[str, Dict[str, Dict[str, List[Tuple[str, str, str]]]]] = {}

        for row in self.mapping:
            if not row['var_name'] in index:
                index[row['var_name']] = {}
            if not row['unison_bridge_id'] in index[row['var_name']]:
                index[row['var_name']][row['unison_bridge_id']] = {}
            index[row['var_name']][row['unison_bridge_id']][row['biobank_value']] = row['export_value']

        for key, val in index.items():
            for key2, val2 in val.items():
                for key3, val3 in val2.items():
                    index[key][key2][key3] = list(set(val3))

        return index
