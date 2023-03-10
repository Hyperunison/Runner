"""
    Unison agent API

    API for opensource nextflow runner agent. A federation node  # noqa: E501

    The version of the OpenAPI document: 1.0.0
    Generated by: https://openapi-generator.tech
"""


import unittest

import auto_api_client
from auto_api_client.api.agent_api import AgentApi  # noqa: E501


class TestAgentApi(unittest.TestCase):
    """AgentApi unit test stubs"""

    def setUp(self):
        self.api = AgentApi()  # noqa: E501

    def tearDown(self):
        pass

    def test_add_run_log_chunk(self):
        """Test case for add_run_log_chunk

        """
        pass

    def test_get_next_task(self):
        """Test case for get_next_task

        """
        pass

    def test_get_types_map(self):
        """Test case for get_types_map

        """
        pass

    def test_set_process_logs(self):
        """Test case for set_process_logs

        """
        pass

    def test_set_run_status(self):
        """Test case for set_run_status

        """
        pass

    def test_update_process_item(self):
        """Test case for update_process_item

        """
        pass


if __name__ == '__main__':
    unittest.main()
