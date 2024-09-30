from sqlalchemy import create_engine

class BaseDatabase:
    connection_string: str
    engine: None

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = create_engine(connection_string, echo=False)