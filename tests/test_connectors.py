import os
import unittest
from unittest import mock

from data_gateway.agent import AgentConfig, AgentService
from data_gateway.connectors import DRIVERS, build_connector


class ConnectorConfigTests(unittest.TestCase):
    def test_only_presto_and_hive(self):
        self.assertEqual(DRIVERS, ("presto", "hive"))
        with self.assertRaises(ValueError):
            build_connector("clickhouse", {"clickhouse": {"host": "h"}})

    def test_host_is_required_in_agent_config(self):
        with self.assertRaises(ValueError):
            build_connector("presto", {})

    def test_credentials_come_from_environment(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                build_connector("hive", {"hive": {"host": "h"}})
        self.assertIn("HADOOP_USER_NAME", str(ctx.exception))

    def test_agent_builds_connector_from_its_connections(self):
        cfg = AgentConfig(name="a", secret="s", relay_url="http://r", token="t",
                          datasets={"d": {"driver": "presto", "sql": "select 1"}},
                          connections={"presto": {"host": "p"}})
        svc = AgentService(cfg)
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):  # reaches the connector, which wants credentials
                svc._factory("presto")


if __name__ == "__main__":
    unittest.main()
