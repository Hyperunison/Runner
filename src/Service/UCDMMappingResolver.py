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

    def transform_mapping_to_resolve_result(self) -> Dict[str, Dict[str, List[Tuple[str, str, str]]]]:
        index: Dict[str, Dict[str, List[Tuple[str, str, str]]]] = {}

        for row in self.mapping:
            if not row['var_name'] in index:
                index[row['var_name']] = {}
            if not row['biobank_value'] in index[row['var_name']]:
                index[row['var_name']][row['biobank_value']] = []
            index[row['var_name']][row['biobank_value']].append(
                (
                    row['export_value'],
                    row['automation_strategy'],
                    row['is_mapped']
                )
            )

        for key, val in index.items():
            for key2, val2 in val.items():
                index[key][key2] = list(set(val2))

        return index
