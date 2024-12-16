from sqlalchemy.exc import ProgrammingError


class NonNumericField(ProgrammingError):
    pass
