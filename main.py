import auto_api_client
from pprint import pprint
from auto_api_client.api import agent_api
import yaml

config = yaml.safe_load(open("config.yaml", "r"))

configuration = auto_api_client.Configuration(
    host = config['apiUrl']
)

with auto_api_client.ApiClient(configuration) as api_client:
    api_instance = agent_api.AgentApi(api_client)

    try:
        api_response = api_instance.get_next_task(str(config['apiVersion']), config['agentToken'])
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_next_task: %s\n" % e)