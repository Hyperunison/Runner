from datetime import datetime

class OMOPoficationBase:
    dir: str = "var/"

    def transform_person_id_to_integer(self, origin_value):
        return int(float(origin_value))

    def transform_float_to_date(self, origin_value):
        ts = self.transform_person_id_to_integer(origin_value)
        date = datetime.fromtimestamp(ts)

        return date.strftime('%Y-%m-%d')

    def transform_float_to_datetime(self, origin_value):
        ts = self.transform_person_id_to_integer(origin_value)
        date = datetime.fromtimestamp(ts)

        return date.strftime('%Y-%m-%d %H:%M:%S')
