# auto_api_client.AgentApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**accept_task**](AgentApi.md#accept_task) | **POST** /api/agent/v{version}/{token}/task/{id} | 
[**add_run_log_chunk**](AgentApi.md#add_run_log_chunk) | **POST** /api/agent/v{version}/{token}/run/{id}/log-chunk | 
[**block_task**](AgentApi.md#block_task) | **POST** /api/agent/v{version}/{token}/task/{id}/block | 
[**export_cdm_values_csv**](AgentApi.md#export_cdm_values_csv) | **GET** /api/agent/v{version}/{token}/{cdmId}/cdm-values/export | 
[**export_cdm_vocabulary_csv**](AgentApi.md#export_cdm_vocabulary_csv) | **GET** /api/agent/v{version}/{token}/{cdmId}/cdm-vocabulary/export | 
[**export_docs_for_specific_biobank**](AgentApi.md#export_docs_for_specific_biobank) | **GET** /api/agent/v{version}/{token}/export-mapping/docs | 
[**export_docs_for_specific_biobank_in_csv_format**](AgentApi.md#export_docs_for_specific_biobank_in_csv_format) | **GET** /api/agent/v{version}/{token}/export-mapping/docs/csv | 
[**export_mapping_short_for_specific_biobank**](AgentApi.md#export_mapping_short_for_specific_biobank) | **GET** /api/agent/v{version}/{token}/export-mapping | 
[**export_mapping_short_for_specific_biobank_json**](AgentApi.md#export_mapping_short_for_specific_biobank_json) | **GET** /api/agent/v{version}/{token}/export-mapping/json | 
[**get_agent_id**](AgentApi.md#get_agent_id) | **GET** /api/agent/v{version}/{token}/agent-id | 
[**get_app_agent_updateprocessitem**](AgentApi.md#get_app_agent_updateprocessitem) | **GET** /api/agent/v{version}/{token}/run/{id}/process | 
[**get_mappings**](AgentApi.md#get_mappings) | **POST** /api/agent/v{version}/{token}/mapping/resolve/{key} | 
[**get_next_task**](AgentApi.md#get_next_task) | **GET** /api/agent/v{version}/{token}/task | 
[**get_types_map**](AgentApi.md#get_types_map) | **GET** /api/agent/v{version}/{token}/next-run/types | 
[**post_app_agent_addjoblogs**](AgentApi.md#post_app_agent_addjoblogs) | **POST** /api/agent/v{version}/{token}/job/runner-message/{runId}/add-logs | 
[**set_car_status**](AgentApi.md#set_car_status) | **POST** /api/agent/v{version}/{token}/car/{id}/status | 
[**set_cohort_definition_aggregation**](AgentApi.md#set_cohort_definition_aggregation) | **POST** /api/agent/v{version}/{token}/cohort/aggregation/{key} | 
[**set_error_for_cohort_api_request**](AgentApi.md#set_error_for_cohort_api_request) | **PUT** /api/agent/v{version}/{token}/cohort/{id}/error | 
[**set_installed_pipelines**](AgentApi.md#set_installed_pipelines) | **POST** /api/agent/v{version}/{token}/pipelines/installed/ | 
[**set_job_state**](AgentApi.md#set_job_state) | **PUT** /api/agent/v{version}/{token}/job/runner-message/{runId}/set-state | 
[**set_kill_result**](AgentApi.md#set_kill_result) | **POST** /api/agent/v{version}/{token}/run/{id}/kill-result | 
[**set_process_logs**](AgentApi.md#set_process_logs) | **POST** /api/agent/v{version}/{token}/process/{processId}/logs | 
[**set_run_dir**](AgentApi.md#set_run_dir) | **POST** /api/agent/v{version}/{token}/run/{id}/dir | 
[**set_run_status**](AgentApi.md#set_run_status) | **POST** /api/agent/v{version}/{token}/run/{id}/status | 
[**set_sql_query_for_cohort_api_request**](AgentApi.md#set_sql_query_for_cohort_api_request) | **PUT** /api/agent/v{version}/{token}/cohort/{id}/sql-query | 
[**set_table_column_freequent_values**](AgentApi.md#set_table_column_freequent_values) | **POST** /api/agent/v{version}/{token}/ucdm/tables/{table}/columns/{column}/values | 
[**set_table_column_stats**](AgentApi.md#set_table_column_stats) | **POST** /api/agent/v{version}/{token}/ucdm/tables/{table}/columns/{column} | 
[**set_table_info**](AgentApi.md#set_table_info) | **PUT** /api/agent/v{version}/{token}/ucdm/tables/{table} | 
[**set_table_stats**](AgentApi.md#set_table_stats) | **POST** /api/agent/v{version}/{token}/ucdm/tables/{table}/columns | 
[**set_tables_list**](AgentApi.md#set_tables_list) | **POST** /api/agent/v{version}/{token}/ucdm/tables | 
[**set_task_error**](AgentApi.md#set_task_error) | **POST** /api/agent/v{version}/{token}/task/{id}/error | 
[**update_process_item**](AgentApi.md#update_process_item) | **POST** /api/agent/v{version}/{token}/run/{id}/process | 


# **accept_task**
> str accept_task(version, token, id)



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
    version = "1" # str | 
    token = "z" # str | 
    id = "id_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.accept_task(version, token, id)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->accept_task: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **id** | **str**|  |

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
**200** | Mark task as completed, not showing it again next time |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **add_run_log_chunk**
> str add_run_log_chunk(id, version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.add_run_log_chunk_request import AddRunLogChunkRequest
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
    token = "z" # str | 
    add_run_log_chunk_request = AddRunLogChunkRequest(
        chunk="",
    ) # AddRunLogChunkRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.add_run_log_chunk(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->add_run_log_chunk: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.add_run_log_chunk(id, version, token, add_run_log_chunk_request=add_run_log_chunk_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->add_run_log_chunk: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Run ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |
 **add_run_log_chunk_request** | [**AddRunLogChunkRequest**](AddRunLogChunkRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List all bricks |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **block_task**
> str block_task(version, token, id)



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
    version = "1" # str | 
    token = "z" # str | 
    id = "id_example" # str | 
    runner_instance = "" # str | uid of runner instance (optional) if omitted the server will use the default value of ""

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.block_task(version, token, id)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->block_task: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.block_task(version, token, id, runner_instance=runner_instance)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->block_task: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **id** | **str**|  |
 **runner_instance** | **str**| uid of runner instance | [optional] if omitted the server will use the default value of ""

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
**200** | Result |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **export_cdm_values_csv**
> file_type export_cdm_values_csv(version, token, cdm_id)



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
    version = "1" # str | 
    token = "z" # str | 
    cdm_id = "cdmId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.export_cdm_values_csv(version, token, cdm_id)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->export_cdm_values_csv: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **cdm_id** | **str**|  |

### Return type

**file_type**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/octet-stream


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Export CDM values to CSV file |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **export_cdm_vocabulary_csv**
> file_type export_cdm_vocabulary_csv(version, token, cdm_id)



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
    version = "1" # str | 
    token = "z" # str | 
    cdm_id = "cdmId_example" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.export_cdm_vocabulary_csv(version, token, cdm_id)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->export_cdm_vocabulary_csv: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **cdm_id** | **str**|  |

### Return type

**file_type**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/octet-stream


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Export CDM vocabulary to CSV file |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **export_docs_for_specific_biobank**
> file_type export_docs_for_specific_biobank(version, token)



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
    version = "1" # str | 
    token = "z" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.export_docs_for_specific_biobank(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->export_docs_for_specific_biobank: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |

### Return type

**file_type**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/octet-stream


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Export biobank mappings docs |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **export_docs_for_specific_biobank_in_csv_format**
> file_type export_docs_for_specific_biobank_in_csv_format(version, token)



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
    version = "1" # str | 
    token = "z" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.export_docs_for_specific_biobank_in_csv_format(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->export_docs_for_specific_biobank_in_csv_format: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |

### Return type

**file_type**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/octet-stream


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Export biobank mappings docs for CSV |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **export_mapping_short_for_specific_biobank**
> file_type export_mapping_short_for_specific_biobank(version, token)



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
    version = "1" # str | 
    token = "z" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.export_mapping_short_for_specific_biobank(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->export_mapping_short_for_specific_biobank: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |

### Return type

**file_type**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/octet-stream


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Export biobank mappings to USAGI csv file |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **export_mapping_short_for_specific_biobank_json**
> [DataItemDictionaryBiobankExportItemResponse] export_mapping_short_for_specific_biobank_json(version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.data_item_dictionary_biobank_export_item_response import DataItemDictionaryBiobankExportItemResponse
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
    token = "z" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.export_mapping_short_for_specific_biobank_json(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->export_mapping_short_for_specific_biobank_json: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |

### Return type

[**[DataItemDictionaryBiobankExportItemResponse]**](DataItemDictionaryBiobankExportItemResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Export biobank mappings to JSON |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_agent_id**
> int get_agent_id(version, token)



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
    version = "1" # str | 
    token = "z" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_agent_id(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_agent_id: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |

### Return type

**int**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Get agent_id |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_app_agent_updateprocessitem**
> str get_app_agent_updateprocessitem(id, version, token)



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
    token = "z" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_app_agent_updateprocessitem(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_app_agent_updateprocessitem: %s\n" % e)
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

# **get_mappings**
> [MappingResolveResponse] get_mappings(version, token, key)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.get_mappings_request import GetMappingsRequest
from auto_api_client.model.mapping_resolve_response import MappingResolveResponse
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
    token = "z" # str | 
    key = "key_example" # str | 
    get_mappings_request = GetMappingsRequest(
        input="",
    ) # GetMappingsRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_mappings(version, token, key)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_mappings: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.get_mappings(version, token, key, get_mappings_request=get_mappings_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_mappings: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **key** | **str**|  |
 **get_mappings_request** | [**GetMappingsRequest**](GetMappingsRequest.md)|  | [optional]

### Return type

[**[MappingResolveResponse]**](MappingResolveResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Resolve all mappings for all variables in single key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

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
    token = "z" # str | 
    queues = [] # [bool, date, datetime, dict, float, int, list, str, none_type] | Filter message by queue (optional) if omitted the server will use the default value of []

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_next_task(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_next_task: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.get_next_task(version, token, queues=queues)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->get_next_task: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **queues** | [**[bool, date, datetime, dict, float, int, list, str, none_type]**](bool, date, datetime, dict, float, int, list, str, none_type.md)| Filter message by queue | [optional] if omitted the server will use the default value of []

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
    token = "z" # str | 

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

# **post_app_agent_addjoblogs**
> post_app_agent_addjoblogs(version, token, run_id)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.post_app_agent_addjoblogs_request import PostAppAgentAddjoblogsRequest
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
    token = "z" # str | 
    run_id = "runId_example" # str | 
    post_app_agent_addjoblogs_request = PostAppAgentAddjoblogsRequest(
        logs="logs_example",
    ) # PostAppAgentAddjoblogsRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_instance.post_app_agent_addjoblogs(version, token, run_id)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_addjoblogs: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_instance.post_app_agent_addjoblogs(version, token, run_id, post_app_agent_addjoblogs_request=post_app_agent_addjoblogs_request)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->post_app_agent_addjoblogs: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **run_id** | **str**|  |
 **post_app_agent_addjoblogs_request** | [**PostAppAgentAddjoblogsRequest**](PostAppAgentAddjoblogsRequest.md)|  | [optional]

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**0** |  |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_car_status**
> str set_car_status(id, version, token)



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
    id = 1 # int | Cohort API Request ID, example: 1234. It may be taken from /next-task API method
    version = "1" # str | 
    token = "z" # str | 
    pid = "" # str | New pid (optional) if omitted the server will use the default value of ""
    status = "success" # str | New Cohort API Request status (optional) if omitted the server will use the default value of "success"

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_car_status(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_car_status: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_car_status(id, version, token, pid=pid, status=status)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_car_status: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Cohort API Request ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |
 **pid** | **str**| New pid | [optional] if omitted the server will use the default value of ""
 **status** | **str**| New Cohort API Request status | [optional] if omitted the server will use the default value of "success"

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
**200** | Set cohort api request status |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_cohort_definition_aggregation**
> str set_cohort_definition_aggregation(version, token, key)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_cohort_definition_aggregation_request import SetCohortDefinitionAggregationRequest
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
    token = "z" # str | 
    key = "key_example" # str | 
    channel = "channel_example" # str | WS channel to send reply (optional)
    raw_only = "rawOnly_example" # str | Is raw only, converting to UPDM will be skipped (optional)
    set_cohort_definition_aggregation_request = SetCohortDefinitionAggregationRequest(
        result="",
        sql="",
    ) # SetCohortDefinitionAggregationRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_cohort_definition_aggregation(version, token, key)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_cohort_definition_aggregation: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_cohort_definition_aggregation(version, token, key, channel=channel, raw_only=raw_only, set_cohort_definition_aggregation_request=set_cohort_definition_aggregation_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_cohort_definition_aggregation: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **key** | **str**|  |
 **channel** | **str**| WS channel to send reply | [optional]
 **raw_only** | **str**| Is raw only, converting to UPDM will be skipped | [optional]
 **set_cohort_definition_aggregation_request** | [**SetCohortDefinitionAggregationRequest**](SetCohortDefinitionAggregationRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Just OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_error_for_cohort_api_request**
> str set_error_for_cohort_api_request(version, token, id)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_task_error_request import SetTaskErrorRequest
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
    token = "z" # str | 
    id = "id_example" # str | 
    set_task_error_request = SetTaskErrorRequest(
        error="",
    ) # SetTaskErrorRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_error_for_cohort_api_request(version, token, id)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_error_for_cohort_api_request: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_error_for_cohort_api_request(version, token, id, set_task_error_request=set_task_error_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_error_for_cohort_api_request: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **id** | **str**|  |
 **set_task_error_request** | [**SetTaskErrorRequest**](SetTaskErrorRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Result |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_installed_pipelines**
> str set_installed_pipelines(version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_installed_pipelines_request import SetInstalledPipelinesRequest
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
    token = "z" # str | 
    set_installed_pipelines_request = SetInstalledPipelinesRequest(
        pipelines=[],
    ) # SetInstalledPipelinesRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_installed_pipelines(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_installed_pipelines: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_installed_pipelines(version, token, set_installed_pipelines_request=set_installed_pipelines_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_installed_pipelines: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **set_installed_pipelines_request** | [**SetInstalledPipelinesRequest**](SetInstalledPipelinesRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Set cohort api request status |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_job_state**
> set_job_state(version, token, run_id)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_job_state_request import SetJobStateRequest
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
    token = "z" # str | 
    run_id = "runId_example" # str | 
    set_job_state_request = SetJobStateRequest(
        state="state_example",
        result="{}",
    ) # SetJobStateRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_instance.set_job_state(version, token, run_id)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_job_state: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_instance.set_job_state(version, token, run_id, set_job_state_request=set_job_state_request)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_job_state: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **run_id** | **str**|  |
 **set_job_state_request** | [**SetJobStateRequest**](SetJobStateRequest.md)|  | [optional]

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**0** |  |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_kill_result**
> str set_kill_result(id, version, token)



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
    token = "z" # str | 
    channel = "channel_example" # str | WS channel to send reply (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_kill_result(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_kill_result: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_kill_result(id, version, token, channel=channel)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_kill_result: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Run ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |
 **channel** | **str**| WS channel to send reply | [optional]

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

# **set_process_logs**
> str set_process_logs(process_id, version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_process_logs_request import SetProcessLogsRequest
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
    process_id = "z" # str | Run ID, example: 1234. It may be taken from /next-task API method
    version = "1" # str | 
    token = "z" # str | 
    channel = "channel_example" # str | WS channel to send reply (optional)
    set_process_logs_request = SetProcessLogsRequest(
        logs="",
    ) # SetProcessLogsRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_process_logs(process_id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_process_logs: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_process_logs(process_id, version, token, channel=channel, set_process_logs_request=set_process_logs_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_process_logs: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **process_id** | **str**| Run ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |
 **channel** | **str**| WS channel to send reply | [optional]
 **set_process_logs_request** | [**SetProcessLogsRequest**](SetProcessLogsRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List all bricks |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_run_dir**
> str set_run_dir(id, version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_run_dir_request import SetRunDirRequest
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
    token = "z" # str | 
    set_run_dir_request = SetRunDirRequest(
        dir="",
    ) # SetRunDirRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_run_dir(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_run_dir: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_run_dir(id, version, token, set_run_dir_request=set_run_dir_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_run_dir: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Run ID, example: 1234. It may be taken from /next-task API method |
 **version** | **str**|  |
 **token** | **str**|  |
 **set_run_dir_request** | [**SetRunDirRequest**](SetRunDirRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Just OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_run_status**
> str set_run_status(id, version, token)



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
    token = "z" # str | 
    status = "success" # str | New run status (optional) if omitted the server will use the default value of "success"

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_run_status(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_run_status: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_run_status(id, version, token, status=status)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_run_status: %s\n" % e)
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

# **set_sql_query_for_cohort_api_request**
> str set_sql_query_for_cohort_api_request(version, token, id)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_sql_query_for_cohort_api_request_request import SetSQLQueryForCohortApiRequestRequest
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
    token = "z" # str | 
    id = "id_example" # str | 
    set_sql_query_for_cohort_api_request_request = SetSQLQueryForCohortApiRequestRequest(
        sql="",
    ) # SetSQLQueryForCohortApiRequestRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_sql_query_for_cohort_api_request(version, token, id)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_sql_query_for_cohort_api_request: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_sql_query_for_cohort_api_request(version, token, id, set_sql_query_for_cohort_api_request_request=set_sql_query_for_cohort_api_request_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_sql_query_for_cohort_api_request: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **id** | **str**|  |
 **set_sql_query_for_cohort_api_request_request** | [**SetSQLQueryForCohortApiRequestRequest**](SetSQLQueryForCohortApiRequestRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Result |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_table_column_freequent_values**
> str set_table_column_freequent_values(version, token, table, column)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_table_column_freequent_values_request import SetTableColumnFreequentValuesRequest
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
    token = "z" # str | 
    table = "z" # str | 
    column = "2" # str | 
    set_table_column_freequent_values_request = SetTableColumnFreequentValuesRequest(
        values=[],
        counts=[],
    ) # SetTableColumnFreequentValuesRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_table_column_freequent_values(version, token, table, column)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_table_column_freequent_values: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_table_column_freequent_values(version, token, table, column, set_table_column_freequent_values_request=set_table_column_freequent_values_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_table_column_freequent_values: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **table** | **str**|  |
 **column** | **str**|  |
 **set_table_column_freequent_values_request** | [**SetTableColumnFreequentValuesRequest**](SetTableColumnFreequentValuesRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Set columns in a table and number of rows |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_table_column_stats**
> str set_table_column_stats(version, token, table, column)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_table_column_stats_request import SetTableColumnStatsRequest
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
    token = "z" # str | 
    table = "z" # str | 
    column = "2" # str | 
    set_table_column_stats_request = SetTableColumnStatsRequest(
        is_private="is_private_example",
        unique_count="unique_count_example",
        nulls_count="nulls_count_example",
        min_value="min_value_example",
        max_value="max_value_example",
        avg_value="avg_value_example",
        median12_value="median12_value_example",
        median25_value="median25_value_example",
        median37_value="median37_value_example",
        median50_value="median50_value_example",
        median63_value="median63_value_example",
        median75_value="median75_value_example",
        median88_value="median88_value_example",
    ) # SetTableColumnStatsRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_table_column_stats(version, token, table, column)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_table_column_stats: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_table_column_stats(version, token, table, column, set_table_column_stats_request=set_table_column_stats_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_table_column_stats: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **table** | **str**|  |
 **column** | **str**|  |
 **set_table_column_stats_request** | [**SetTableColumnStatsRequest**](SetTableColumnStatsRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Set columns in a table and number of rows |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_table_info**
> str set_table_info(version, token, table)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_table_info_request import SetTableInfoRequest
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
    token = "z" # str | 
    table = "z" # str | 
    set_table_info_request = SetTableInfoRequest(
        abandoned="abandoned_example",
    ) # SetTableInfoRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_table_info(version, token, table)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_table_info: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_table_info(version, token, table, set_table_info_request=set_table_info_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_table_info: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **table** | **str**|  |
 **set_table_info_request** | [**SetTableInfoRequest**](SetTableInfoRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Update table info, like abandon status |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_table_stats**
> str set_table_stats(version, token, table)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_table_stats_request import SetTableStatsRequest
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
    token = "z" # str | 
    table = "z" # str | 
    set_table_stats_request = SetTableStatsRequest(
        columns=[],
        types=[],
        nullable=[],
        rows_count="rows_count_example",
    ) # SetTableStatsRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_table_stats(version, token, table)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_table_stats: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_table_stats(version, token, table, set_table_stats_request=set_table_stats_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_table_stats: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **table** | **str**|  |
 **set_table_stats_request** | [**SetTableStatsRequest**](SetTableStatsRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Set columns in a table and number of rows |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_tables_list**
> [BiobankDataTable] set_tables_list(version, token)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_tables_list_request import SetTablesListRequest
from auto_api_client.model.biobank_data_table import BiobankDataTable
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
    token = "z" # str | 
    set_tables_list_request = SetTablesListRequest(
        tables=[],
    ) # SetTablesListRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_tables_list(version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_tables_list: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_tables_list(version, token, set_tables_list_request=set_tables_list_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_tables_list: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **set_tables_list_request** | [**SetTablesListRequest**](SetTablesListRequest.md)|  | [optional]

### Return type

[**[BiobankDataTable]**](BiobankDataTable.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Upsert list of tables available in biobank for cohort API |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_task_error**
> str set_task_error(version, token, id)



### Example


```python
import time
import auto_api_client
from auto_api_client.api import agent_api
from auto_api_client.model.set_task_error_request import SetTaskErrorRequest
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
    token = "z" # str | 
    id = "id_example" # str | 
    set_task_error_request = SetTaskErrorRequest(
        error="",
    ) # SetTaskErrorRequest |  (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.set_task_error(version, token, id)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_task_error: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.set_task_error(version, token, id, set_task_error_request=set_task_error_request)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->set_task_error: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **version** | **str**|  |
 **token** | **str**|  |
 **id** | **str**|  |
 **set_task_error_request** | [**SetTaskErrorRequest**](SetTaskErrorRequest.md)|  | [optional]

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Result |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_process_item**
> str update_process_item(id, version, token)



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
    token = "z" # str | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.update_process_item(id, version, token)
        pprint(api_response)
    except auto_api_client.ApiException as e:
        print("Exception when calling AgentApi->update_process_item: %s\n" % e)
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

