# auto-api-client
API for opensource nextflow runner agent. A federation node

This Python package is automatically generated by the [OpenAPI Generator](https://openapi-generator.tech) project:

- API version: 1.0.0
- Package version: 1.0.0
- Build package: org.openapitools.codegen.languages.PythonPriorClientCodegen

## Requirements.

Python >=3.6

## Installation & Usage
### pip install

If the python package is hosted on a repository, you can install directly using:

```sh
pip install git+https://github.com/GIT_USER_ID/GIT_REPO_ID.git
```
(you may need to run `pip` with root permission: `sudo pip install git+https://github.com/GIT_USER_ID/GIT_REPO_ID.git`)

Then import the package:
```python
import auto_api_client
```

### Setuptools

Install via [Setuptools](http://pypi.python.org/pypi/setuptools).

```sh
python setup.py install --user
```
(or `sudo python setup.py install` to install the package for all users)

Then import the package:
```python
import auto_api_client
```

## Getting Started

Please follow the [installation procedure](#installation--usage) and then run the following:

```python

import time
import auto_api_client
from pprint import pprint
from auto_api_client.api import agent_api
from auto_api_client.model.add_run_log_chunk_request import AddRunLogChunkRequest
from auto_api_client.model.runner_message import RunnerMessage
from auto_api_client.model.types_map import TypesMap
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = auto_api_client.Configuration(
    host = "http://localhost"
)



# Enter a context with an instance of the API client
with auto_api_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = agent_api.AgentApi(api_client)
    id = 1 # int | Run ID, example: 1234. It may be taken from /next-task API method
    version = "1" # str | 
    token = "f" # str | 
    add_run_log_chunk_request = AddRunLogChunkRequest(
        chunk="",
    ) # AddRunLogChunkRequest |  (optional)

    try:
        api_response = api_instance.add_run_log_chunk(id, version, token, add_run_log_chunk_request=add_run_log_chunk_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->add_run_log_chunk: %s\n" % e)
```

## Documentation for API Endpoints

All URIs are relative to *http://localhost*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*AgentApi* | [**add_run_log_chunk**](docs/AgentApi.md#add_run_log_chunk) | **POST** /api/agent/v{version}/{token}/run/{id}/log-chunk | 
*AgentApi* | [**get_next_task**](docs/AgentApi.md#get_next_task) | **GET** /api/agent/v{version}/{token}/next-task | 
*AgentApi* | [**get_types_map**](docs/AgentApi.md#get_types_map) | **GET** /api/agent/v{version}/{token}/next-run/types | 
*AgentApi* | [**set_process_logs**](docs/AgentApi.md#set_process_logs) | **POST** /api/agent/v{version}/{token}/run/{id}/process/logs | 
*AgentApi* | [**set_run_status**](docs/AgentApi.md#set_run_status) | **POST** /api/agent/v{version}/{token}/run/{id}/status | 
*AgentApi* | [**update_process_item**](docs/AgentApi.md#update_process_item) | **POST** /api/agent/v{version}/{token}/run/{id}/process | 


## Documentation For Models

 - [AddRunLogChunkRequest](docs/AddRunLogChunkRequest.md)
 - [GetProcessLogs](docs/GetProcessLogs.md)
 - [NextflowRun](docs/NextflowRun.md)
 - [RunnerMessage](docs/RunnerMessage.md)
 - [TypesMap](docs/TypesMap.md)


## Documentation For Authorization

 All endpoints do not require authorization.

## Author




## Notes for Large OpenAPI documents
If the OpenAPI document is large, imports in auto_api_client.apis and auto_api_client.models may fail with a
RecursionError indicating the maximum recursion limit has been exceeded. In that case, there are a couple of solutions:

Solution 1:
Use specific imports for apis and models like:
- `from auto_api_client.api.default_api import DefaultApi`
- `from auto_api_client.model.pet import Pet`

Solution 2:
Before importing the package, adjust the maximum recursion limit as shown below:
```
import sys
sys.setrecursionlimit(1500)
import auto_api_client
from auto_api_client.apis import *
from auto_api_client.models import *
```

