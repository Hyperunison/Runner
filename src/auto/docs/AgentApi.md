# auto_api_client.AgentApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_next_task**](AgentApi.md#get_next_task) | **GET** /api/agent/v{version}/{token}/next-task | 
[**get_types_map**](AgentApi.md#get_types_map) | **GET** /api/agent/v{version}/{token}/next-run/types | 
[**post_app_agent_addrunlogchunk**](AgentApi.md#post_app_agent_addrunlogchunk) | **POST** /api/agent/v{version}/{token}/run/{id}/log-chunk | 
[**post_app_agent_setprocesslogs**](AgentApi.md#post_app_agent_setprocesslogs) | **POST** /api/agent/v{version}/{token}/run/{id}/process/logs | 
[**post_app_agent_setrunstatus**](AgentApi.md#post_app_agent_setrunstatus) | **POST** /api/agent/v{version}/{token}/run/{id}/status | 
[**post_app_agent_updateprocessitem**](AgentApi.md#post_app_agent_updateprocessitem) | **POST** /api/agent/v{version}/{token}/run/{id}/process | 


# **get_next_task**
> RunnerMessage get_next_task(version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.runner_message import RunnerMessage
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = auto_api_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with auto_api_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = agent_api.AgentApi(api_client)
    version = "1" # str | 
    token = "f" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_next_task(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_next_task: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |

### Return type

[**RunnerMessage**](RunnerMessage.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Get next task for runner agent |  -  |
**204** | No tasks |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_types_map**
> TypesMap get_types_map(version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.types_map import TypesMap
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = auto_api_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with auto_api_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = agent_api.AgentApi(api_client)
    version = "1" # str | 
    token = "f" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_types_map(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_types_map: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |

### Return type

[**TypesMap**](TypesMap.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Map of data types |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **post_app_agent_addrunlogchunk**
> str post_app_agent_addrunlogchunk(id, version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = auto_api_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with auto_api_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = agent_api.AgentApi(api_client)
    id = 1 # int | Run ID, example: 1234. It may be taken from /next-task API method
    version = "1" # str | 
    token = "f" # str | 
    chunk = "" # str | Logs chunk to be attached (optional) if omitted the server will use the default value of ""

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.post_app_agent_addrunlogchunk(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_addrunlogchunk: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.post_app_agent_addrunlogchunk(id, version, token, chunk=chunk)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_addrunlogchunk: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Run ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |
 **chunk** | **str**| Logs chunk to be attached | [optional] if omitted the server will use the default value of ""

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List all bricks |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **post_app_agent_setprocesslogs**
> str post_app_agent_setprocesslogs(id, version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = auto_api_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with auto_api_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = agent_api.AgentApi(api_client)
    id = 1 # int | Run ID, example: 1234. It may be taken from /next-task API method
    version = "1" # str | 
    token = "f" # str | 
     = "logs" # str | New run status (optional) if omitted the server will use the default value of "logs"

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.post_app_agent_setprocesslogs(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_setprocesslogs: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.post_app_agent_setprocesslogs(id, version, token, =)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_setprocesslogs: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Run ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |
 **** | **str**| New run status | [optional] if omitted the server will use the default value of "logs"

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List all bricks |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **post_app_agent_setrunstatus**
> str post_app_agent_setrunstatus(id, version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = auto_api_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with auto_api_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = agent_api.AgentApi(api_client)
    id = 1 # int | Run ID, example: 1234. It may be taken from /next-task API method
    version = "1" # str | 
    token = "f" # str | 
    status = "success" # str | New run status (optional) if omitted the server will use the default value of "success"

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.post_app_agent_setrunstatus(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_setrunstatus: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.post_app_agent_setrunstatus(id, version, token, status=status)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_setrunstatus: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Run ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |
 **status** | **str**| New run status | [optional] if omitted the server will use the default value of "success"

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List all bricks |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **post_app_agent_updateprocessitem**
> str post_app_agent_updateprocessitem(id, version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = auto_api_client.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with auto_api_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = agent_api.AgentApi(api_client)
    id = 1 # int | Run ID, example: 1234. It may be taken from /next-task API method
    version = "1" # str | 
    token = "f" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.post_app_agent_updateprocessitem(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_updateprocessitem: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Run ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List all bricks |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

