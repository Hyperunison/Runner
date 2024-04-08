import signal
import os
import argparse
import socket

import sentry_sdk
from src.auto.auto_api_client.configuration import Configuration
from src.LogConfigurator import configure_logs


def child_handler(signum, frame):
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid <= 0:
                break
        except OSError:
            break


class ConsoleApplicationManager:
    args: argparse.Namespace

    def __init__(self):
        pass

    def initialize(self, config: any) -> Configuration:
        signal.signal(signal.SIGCHLD, child_handler)
        configure_logs(config, "main")
        configuration = Configuration(host=config['api_url'])

        if 'sentry_dsn' in config:
            sentry_sdk.init(
                dsn=config['sentry_dsn'],
                enable_tracing=True,
                server_name=os.getenv('CONTAINER_NAME', socket.gethostname()),
                environment=os.getenv('ENV', 'production')
            )

        self.parse_cli_args()
        return configuration

    def parse_cli_args(self):
        parser = argparse.ArgumentParser(
            prog='Unison Biobank Runner',
            description='Program interacts with local database and Hyperunison',
            epilog='')
        parser.add_argument('-s', '--skip_accept', action='store_true')
        self.args = parser.parse_args()

