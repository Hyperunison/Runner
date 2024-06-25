import logging
from src.Service.ConfigurationLoader import ConfigurationLoader
import time
import auto_api_client
import socket
from src.Service.ConsoleApplicationManager import ConsoleApplicationManager
from src.Service.NextflowCohortWorkflowExecutor import NextflowCohortWorkflowExecutor
from src.UCDM.DataSchema import DataSchema
from src.auto.auto_api_client.api import agent_api
from src.Api import Api
from src.Adapters.AdapterFactory import create_by_config
from src.Message.CohortAPIRequest import CohortAPIRequest
from src.Message.KillCohortAPIRequest import KillCohortAPIRequest
from src.Message.KillJob import KillJob
from src.Message.GetProcessLogs import GetProcessLogs
from src.Message.NextflowRun import NextflowRun
from src.Message.StartWorkflow import StartWorkflow
from src.Message.UpdateTablesList import UpdateTablesList
from src.Message.UpdateTableColumnsList import UpdateTableColumnsList
from src.Message.UpdateTableColumnStats import UpdateTableColumnStats
from src.Message.StartOMOPoficationWorkflow import StartOMOPoficationWorkflow
from src.Service.MessageFactory import MessageFactory
from src.auto.auto_api_client.api_client import ApiClient

try:
    import pydevd_pycharm

    pydevd_pycharm.settrace('host.docker.internal', port=55147, stdoutToServer=True, stderrToServer=True)
except:
    pass

config = ConfigurationLoader("config.yaml").get_config()
allow_private_upload_data_to_unison = config['allow_private_upload_data_to_unison'] == 1

manager = ConsoleApplicationManager()
configuration = manager.initialize(config)

with ApiClient(configuration) as api_client:
    runner_instance_id = socket.gethostname()
    api_instance = agent_api.AgentApi(api_client)
    api = Api(api_instance, config['api_version'], config['agent_token'])
    adapter = create_by_config(api, config, runner_instance_id)
    schema = DataSchema(config['phenoenotypicDb']['dsn'], config['phenoenotypicDb']['schema'],
                        config['phenoenotypicDb']['min_count'])
    workflow_executor = NextflowCohortWorkflowExecutor(api, adapter, schema)
    check_interval = config['check_runs_status_interval']
    last_check = None
    while True:
        response = None
        try:
            if not last_check or time.time() - last_check > check_interval:
                adapter.check_runs_statuses()
                last_check = time.time()
            response = api.next_task()
            message = MessageFactory().create_message_object_from_response(message=response)
            if not message is None:
                logging.info("Received message {}".format(response.type))
                if not manager.args.skip_accept:
                    result = api.block_task(response.id, runner_instance_id)
                    if result != 'ok':
                        continue
            if type(message) is NextflowRun:
                adapter.process_nextflow_run(message)
            elif type(message) is GetProcessLogs:
                adapter.process_get_process_logs(message)
            elif type(message) is KillJob:
                adapter.process_kill_job(message)
            elif type(message) is CohortAPIRequest:
                schema.execute_cohort_definition(message, api)
            elif type(message) is KillCohortAPIRequest:
                schema.kill_cohort_definition(message, api)
            elif type(message) is UpdateTablesList:
                schema.update_tables_list(api, config['data_protected']['schemas'], config['data_protected']['tables'])
            elif type(message) is UpdateTableColumnsList:
                schema.update_table_columns_list(api, message, config['data_protected']['columns'])
            elif type(message) is UpdateTableColumnStats:
                schema.update_table_column_stats(api, message, config['phenoenotypicDb']['min_count'],
                                                 config['data_protected']['tables'],
                                                 config['data_protected']['columns'])
            elif type(message) is StartWorkflow:
                workflow_executor.execute_workflow(message, allow_private_upload_data_to_unison)
            elif type(message) is StartOMOPoficationWorkflow:
                workflow_executor.execute_workflow(message, allow_private_upload_data_to_unison)
            elif message is None:
                if False:
                    logging.debug("idle, do nothing")
            else:
                error = "Unknown message type {}".format(type(message))
                if not response is None and not response.id is None:
                    api.set_task_error(response.id, error)

            if message is not None:
                if not manager.args.skip_accept:
                    api.accept_task(response.id)
            else:
                time.sleep(config['idle_delay'])
        except auto_api_client.ApiException as e:
            error = "Exception when calling AgentApi: %s\n" % e
            logging.critical(error)
            if not response is None and not response.id is None:
                api.set_task_error(response.id, error)

        except Exception as e:
            error = "Unknown exception: %s\n" % e
            logging.critical(error)
            if not response is None and not response.id is None:
                api.set_task_error(response.id, error)
            raise e
