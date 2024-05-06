import logging
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['log.level'] = record.levelname
        del log_record['level']
        log_record['@timestamp'] = record.asctime
        del log_record['asctime']


def configure_logs(config, message: str = ''):
    level = int(config['log']['level'])
    format_name = config['log']['format'] if 'format' in config['log'] else 'raw'
    if format_name == 'json':
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler())
        log_formatter = CustomJsonFormatter('%(@timestamp)s %(asctime)s %(log.level)s %(level)s %(name)s %(message)s')
    else:
        format ='%(asctime)s [%(levelname)s] [{}] %(message)s'.format(message)
        log_formatter=logging.Formatter(format)
        logging.basicConfig(level=level, format=format)
        logging.getLogger().setLevel(level)

    for handler in logging.getLogger().handlers:
        handler.setFormatter(log_formatter)

    logging.basicConfig()
    logging.getLogger().setLevel(level)
    requests_log = logging.getLogger("auto_api_client.rest")
    requests_log.setLevel(level)
    requests_log.propagate = True

    urllib_log = logging.getLogger("http.client")
    urllib_log.setLevel(level)
    urllib_log.propagate = True
