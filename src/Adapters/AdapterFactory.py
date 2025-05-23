from src.Adapters.DockerAdapter import DockerAdapter
from src.Adapters.K8sAdapter import K8sAdapter
from src.Adapters.NoneAdapter import NoneAdapter
from src.Service.Workflows.PipelineExecutor import PipelineExecutor


def create_by_config(api_client, config, runner_instance_id, agent_id = None) -> PipelineExecutor:
    adapter = None
    if agent_id is None:
        agent_id = api_client.get_agent_id()
    if config['pipeline']['executor']['type'] == 'k8s':
        adapter = K8sAdapter(api_client, runner_instance_id, config['pipeline'], agent_id)
    if config['pipeline']['executor']['type'] == 'docker':
        adapter = DockerAdapter(api_client, runner_instance_id, config['pipeline'], agent_id)
    if config['pipeline']['executor']['type'] == 'none':
        adapter = NoneAdapter()

    if adapter is None:
        raise Exception("Unknown adapter type " + config['pipeline']['executor']['type'])

    return PipelineExecutor(adapter, api_client, config['pipeline'], agent_id)
