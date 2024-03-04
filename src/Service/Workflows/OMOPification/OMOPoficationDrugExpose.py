from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationDrugExpose(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, str]]):
        header = []
        filename = self.dir + "/drug_expose.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                writer.writerow(output)