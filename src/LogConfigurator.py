import logging
import sys


def configure_logs(config, message: str = ''):
    level = int(config['log']['level'])
    logging.basicConfig(level=level, format='%(asctime)s [%(levelname)s] [{}] %(message)s'.format(message))
    logging.getLogger().setLevel(level)

    for handler in logging.getLogger().handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [{}] %(message)s'.format(message)))

    logging.basicConfig()
    logging.getLogger().setLevel(level)
    requests_log = logging.getLogger("auto_api_client.rest")
    requests_log.setLevel(level)
    requests_log.propagate = True

    urllib_log = logging.getLogger("http.client")
    urllib_log.setLevel(level)
    urllib_log.propagate = True
