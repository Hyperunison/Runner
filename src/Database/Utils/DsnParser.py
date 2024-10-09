from urllib.parse import urlparse

class DsnParser:

    def get_engine_type(self, dsn: str) -> str:
        parsed_dsn = urlparse(dsn)
        return parsed_dsn.scheme.split('+')[0]