import logging
import sentry_sdk
import yaml
import time
import auto_api_client
import socket
import sys

from src.Service.NextflowCohortWorkflowExecutor import NextflowCohortWorkflowExecutor
from src.auto.auto_api_client.api import agent_api
from src.Api import Api
from src.Adapters.AdapterFactory import create_by_config
from src.LogConfigurator import configure_logs
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.Message.KillJob import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun
from src.Message.StartWorkflow import StartWorkflow
from src.Message.StartMLTrain import StartMLTrain
from src.MessageFactory import MessageFactory
from src.UCDM.Factory import create_schema_by_config

config = yaml.safe_load(open("config.yaml", "r"))

configure_logs(config, "main")
configuration = auto_api_client.Configuration(host=config['api_url'])

if 'sentry_dsn' in config:
    sentry_sdk.init(
        dsn= config['sentry_dsn'],
    )

with auto_api_client.ApiClient(configuration) as api_client:
    runner_instance_id = socket.gethostname()
    api_instance = agent_api.AgentApi(api_client)
    api = Api(api_instance, config['api_version'], config['agent_token'])
    adapter = create_by_config(api, config, runner_instance_id)
    schema = create_schema_by_config(config['phenoenotypicDb'])
    workflow_executor = NextflowCohortWorkflowExecutor(api, adapter, schema)
    check_interval = config['check_runs_status_interval']
    last_check = None
    while True:
        try:
            if not last_check or time.time() - last_check > check_interval:
                adapter.check_runs_statuses()
                last_check = time.time()
            response = api.next_task()
            message = MessageFactory().create_message_object_from_response(message=response)
            if not message is None:
                logging.info("Received message {}".format(response.type))
                result = api.block_task(response.id, runner_instance_id)
                if not result['ok']:
                    continue
            if type(message) is NextflowRun:
                api_instance.api_client.close()
                api_instance.api_client.rest_client.pool_manager.clear()
                adapter.process_nextflow_run(message)
                api.accept_task(response.id)
            elif type(message) is GetProcessLogs:
                adapter.process_get_process_logs(message)
            elif type(message) is KillJob:
                adapter.process_kill_job(message)
            elif type(message) is CohortAPIRequest:
                schema.execute_cohort_definition(message, api)
            elif type(message) is StartWorkflow:
                workflow_executor.execute_workflow(message)
            elif message is None:
                if False:
                    logging.debug("idle, do nothing")
            else:
                logging.error("Unknown message type {}".format(type(message)))

            if message is not None:
                api.accept_task(response.id)

            time.sleep(config['idle_delay'])
        except auto_api_client.ApiException as e:
            logging.critical("Exception when calling AgentApi: %s\n" % e)
        except Exception as e:
            logging.critical("Unknown exception: %s\n" % e)
            sys.exit(1)
