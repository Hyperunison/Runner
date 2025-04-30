import logging
import os
import time

from src.Service.ConfigurationLoader import ConfigurationLoader
from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.ThreadsManager.ThreadsManager import ThreadsManager

try:
    import pydevd_pycharm

    pydevd_pycharm.settrace('host.docker.internal', port=55147, stdoutToServer=True, stderrToServer=True)
except:
    pass

config = ConfigurationLoader("config.yaml").get_config()
manager = ConsoleApplicationManager()
configuration = manager.initialize(config)

logging.info("Start listening tasks from the server, the main thread ID is {}".format(os.getpid()))
threads_manager = ThreadsManager(config, manager)
threads_manager.send_pipelines()
while True:
    threads_manager.run_next_threads()
    time.sleep(config['idle_delay'])