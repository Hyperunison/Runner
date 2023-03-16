import logging
import os
import sys

import auto_api_client
from auto_api_client.api import agent_api
import yaml
import time
from src.Api import Api
from src.Adapters.AdapterFactory import create_by_config
from src.LogConfigurator import configure_logs
from src.Message.KillJob import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun
from src.MessageFactory import MessageFactory

config = yaml.safe_load(open("config.yaml", "r"))

configure_logs(config, "main")
configuration = auto_api_client.Configuration(host=config['api_url'])

with auto_api_client.ApiClient(configuration) as api_client:
    api_instance = agent_api.AgentApi(api_client)
    api = Api(api_instance, config['api_version'], config['agent_token'])
    adapter = create_by_config(api, config)
    while True:
        try:
            response = api.next_task()
            message = MessageFactory().create_message_object_from_response(message=response)
            logging.info("Received message {}".format(response.type))

            if type(message) is NextflowRun:
                api_instance.api_client.close()
                api_instance.api_client.rest_client.pool_manager.clear()
                pid = os.fork()

                if pid == 0:
                    configure_logs(config, "child={}".format(os.getpid()))
                    logging.info("Forked, run nextflow in fork, pid={}".format(os.getpid()))
                    adapter.process_nextflow_run(message)
                    logging.info("Exiting child")
                    sys.exit(0)

            elif type(message) is GetProcessLogs:
                adapter.process_get_process_logs(message)
            elif type(message) is KillJob:
                adapter.process_kill_job(message)
            elif message is None:
                logging.debug("idle, do nothing")
            else:
                logging.error("Unknown message type {}".format(type(message)))

            time.sleep(config['idle_delay'])
        except auto_api_client.ApiException as e:
            logging.critical("Exception when calling AgentApi: %s\n" % e)
