from typing import List, Dict

from src.Service.Workflows.OMOPification.OMOPoficationBase import OMOPoficationBase
import csv


class OMOPoficationObservationPeriod(OMOPoficationBase):

    def build(self, ucdm: List[Dict[str, str]]):
        header = []
        filename = self.dir + "/observation_period.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Writes the keys as headers
            for row in ucdm:
                output = {}
                writer.writerow(output)