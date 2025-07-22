import logging
import os
import socket
import sys
import time

from src.Api import Api
from src.Service.ConfigurationLoader import ConfigurationLoader
from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.Service.MessageFactory import MessageFactory
from src.ThreadsManager.ThreadsManager import ThreadsManager
from src.auto.auto_api_client.api import agent_api
from src.auto.auto_api_client.api_client import ApiClient


if __name__ == '__main__':
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
    if threads_manager.agent_id is None:
        logging.error("Runner token is invalid")
        sys.exit(1)
    threads_manager.send_pipelines()

    with ApiClient(configuration) as api_client:
        runner_instance_id = socket.gethostname() + "-" + str(os.getpid())
        api_instance = agent_api.AgentApi(api_client)
        api = Api(api_instance, config['api_version'], config['agent_token'])

        while True:
            threads_manager.update_workers_status()
            queues = threads_manager.get_not_depleted_queues()
            if len(queues) == 0:
                logging.debug("All workers are busy, waiting")
                time.sleep(config['idle_delay'])
                continue
            task = api.next_task(queues)
            message = MessageFactory().create_message_object_from_response(message=task)
            if message is None:
                time.sleep(config['idle_delay'])
                continue
            logging.debug("Received message {}".format(task.type))
            threads_manager.terminate_timeouted_threads()
            threads_manager.execute_task(task, message)
