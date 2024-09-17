from typing import Dict
import logging
import os
import csv

class StrToIntGenerator:
    map: Dict[str, int] = {}
    counter: int = 0
    mapping_file_path: str = "var/str-to-int.csv"
    mapping_file_header = ['String', 'Integer']

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

    def load_from_file(self):
        if not os.path.exists(self.mapping_file_path):
            return
        with open(self.mapping_file_path, 'r', newline='') as file:
            reader = csv.reader(file, delimiter=',', quotechar='"')
            try:
                next(reader)
            except StopIteration:
                return
            for row in reader:
                self.map[row[0]] = int(row[1])
            self.counter = len(self.map.values())

    def save_to_file(self):
        with open(self.mapping_file_path, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.mapping_file_header)
            writer.writeheader()

            for key, value in self.map.items():
                writer.writerow({'String': key, 'Integer': value})
