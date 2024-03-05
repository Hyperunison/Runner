class OMOPoficationBase:
    dir: str = "var/"

    def transform_person_id_to_integer(self, origin_value):
        return int(float(origin_value))
