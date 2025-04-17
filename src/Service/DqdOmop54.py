import urllib.request
import urllib.error
from typing import Dict
import json


class DqdOmop54:
    namespace: str = None

    def __init__(self):
        pass

    def generate_results_json(self, sqlite_filename: str, run_id: int) -> Dict[str, str]:
        try:
            url = "http://dqd-omop-5.4:8080/dqd-omop-5.4?file=" + sqlite_filename + "&id=" + str(run_id)
            request = urllib.request.Request(url, data=b'', method='POST')

            # Send the request and get the response
            with urllib.request.urlopen(request) as response:
                status_code = response.getcode()
                if status_code == 200:
                    json_str = response.read().decode('utf-8')
                    data = json.loads(json_str)
                    data['result'] = "/app/var/" + data['result']
                    data['log'] = "/app/var/" + data['log']
                    data['success'] = True

                    return data
                else:
                    data = {}
                    data['success'] = False
                    data['log'] = "/app/var/qc-" + str(run_id) + '.log'

                    return data
        except Exception as e:
            data = {}
            data['success'] = False
            data['log'] = "/app/var/qc-" + str(run_id) + '.log'

            return data