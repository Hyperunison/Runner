from datetime import datetime
from typing import List, Dict

class OMOPoficationBase:
    dir: str = "var/"
    error_report_dir: str = "var/errorReport/"
    error_rows: List[Dict]

    def transform_person_id_to_integer(self, origin_value):
        return int(float(self.clear_person_id(origin_value)))

    def transform_float_to_date(self, origin_value):
        ts = self.transform_person_id_to_integer(origin_value)
        date = datetime.fromtimestamp(ts)

        return date.strftime('%Y-%m-%d')

    def transform_float_to_datetime(self, origin_value):
        ts = self.transform_person_id_to_integer(origin_value)
        date = datetime.fromtimestamp(ts)

        return date.strftime('%Y-%m-%d %H:%M:%S')

    def get_dir(self):
        return self.dir

    def clear_person_id(self, origin_value):
        return origin_value.replace("DKFZ-I00", "")

    def add_row_to_error_rows(self, row):
        self.error_rows.append(row)

