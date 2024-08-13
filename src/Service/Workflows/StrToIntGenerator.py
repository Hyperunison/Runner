from typing import Dict
import logging


class StrToIntGenerator:
    map: Dict[str, int] = {}
    counter: int = 0

    def __init__(self):
        self.counter = 0
        self.map = {}

    def get_int(self, string: any) -> int:
        string: str = str(string)
        if string in self.map:
            logging.debug("Resolve {} for str {} from memory".format(self.map[string], string))
            return self.map[string]

        self.counter += 1
        self.map[string] = self.counter

        logging.debug("Resolve new value {} for str {}. Dict len={}".format(self.counter, string, len(self.map.values())))

        return self.counter
