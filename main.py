import auto_api_client
from pprint import pprint
from auto_api_client.api import agent_api
import yaml
import time
from src.Api import Api
from src.Adapters.AdapterFactory import create_by_config

config = yaml.safe_load(open("config.yaml", "r"))

configuration = auto_api_client.Configuration(host=config['api_url'])

with auto_api_client.ApiClient(configuration) as api_client:
    api_instance = agent_api.AgentApi(api_client)
    api = Api(api_instance, config['api_version'], config['agent_token'])
    adapter = create_by_config(api, config)
    while True:
        try:
            api_response = api.next_task()
            pprint(api_response)
            time.sleep(config['idle_delay'])
            print(adapter.type())
        except auto_api_client.ApiException as e:
            print("Exception when calling AgentApi->get_next_task: %s\n" % e)
