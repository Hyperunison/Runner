import urllib.request
import urllib.error
from typing import Dict
import json


class DqdOmop54:
    namespace: str = None

    def __init__(self):
        pass

    def generate_results_json(self, sqlite_filename: str) -> Dict[str, str]:
        url = "http://dqd-omop-5.4:8080/dqd-omop-5.4?file=" + sqlite_filename
        request = urllib.request.Request(url, data=b'', method='POST')

        # Send the request and get the response
        with urllib.request.urlopen(request) as response:
            status_code = response.getcode()
            if status_code == 200:
                json_str = response.read().decode('utf-8')
                data = json.loads(json_str)
                data['result'] = "/app/var/" + data['result']
                data['log'] = "/app/var/" + data['log']
                return data
            else:
                response = response.read().decode('utf-8')
                raise Exception('Error during generation DQD: {}'.format(response))
