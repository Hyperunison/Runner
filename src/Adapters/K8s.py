from src.Adapters.BaseAdapter import BaseAdapter


class K8s(BaseAdapter):
    namespace = None
    masterPod = None
    api_client = None

    def __init__(self, api_client, config):
        print(config)
        self.namespace = config['namespace']
        self.masterPod = config['master_pod']
        self.api_client = api_client

    def type(self):
        return 'k8s'
