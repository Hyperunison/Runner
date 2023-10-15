import logging
import sys


def configure_logs(config, message: str = ''):
    level = int(config['log']['level'])
    format = "{'time':'%(asctime)s', 'name': '%(name)s', 'level': '%(levelname)s', 'message': '%(message)s'}"
    file_formatter=logging.Formatter(format)
    logging.basicConfig(level=level, format=format)
    logging.getLogger().setLevel(level)

    for handler in logging.getLogger().handlers:
        handler.setFormatter(file_formatter)

    logging.basicConfig()
    logging.getLogger().setLevel(level)
    requests_log = logging.getLogger("auto_api_client.rest")
    requests_log.setLevel(level)
    requests_log.propagate = True

    urllib_log = logging.getLogger("http.client")
    urllib_log.setLevel(level)
    urllib_log.propagate = True
