import logging
import sys


def configure_logs(config):
    level = int(config['log']['level'])
    logging.basicConfig(level=level, format='%(asctime)s [%(levelname)s] %(message)s')
    logging.getLogger().setLevel(level)

    logging.basicConfig()
    logging.getLogger().setLevel(level)
    requests_log = logging.getLogger("auto_api_client.rest")
    requests_log.setLevel(level)
    requests_log.propagate = True

    urllib_log = logging.getLogger("http.client")
    urllib_log.setLevel(level)
    urllib_log.propagate = True
