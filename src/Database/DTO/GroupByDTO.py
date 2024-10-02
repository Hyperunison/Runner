from typing import List

class GroupByDTO:
    fields: List[str]

    def __init__(self, fields: List[str]):
        self.fields = fields