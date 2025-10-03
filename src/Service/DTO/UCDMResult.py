from typing import List, Dict

from src.Service.UCDMConvertedField import UCDMConvertedField


class UCDMResult:
    lines: List[Dict[str, UCDMConvertedField]]
    traceability: List[Dict[str, UCDMConvertedField]]

    def __init__(self):
        self.lines = []
        self.traceability = []