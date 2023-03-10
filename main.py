import logging

import auto_api_client
from auto_api_client.api import agent_api
import yaml
import time
from src.Api import Api
from src.Adapters.AdapterFactory import create_by_config
from src.Message.CreateFolder import CreateFolder
from src.Message.NextflowRun import NextflowRun
from src.MessageFactory import MessageFactory

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

config = yaml.safe_load(open("config.yaml", "r"))

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
                adapter.process_nextflow_run(message)
            elif type(message) is CreateFolder:
                adapter.process_create_folder(message)
            elif message is None:
                logging.debug("idle, do nothing")
            else:
                logging.error("Unknown message type {}".format(type(message)))

            time.sleep(config['idle_delay'])
        except auto_api_client.ApiException as e:
            logging.critical("Exception when calling AgentApi\n")
