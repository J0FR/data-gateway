"""Presto and Hive connectors (adapted from didi99.bigdata.connectors).

Hosts, ports and queues come from the agent's local config (``connections:`` in
agent.yaml), never from this repository. Credentials come from the environment:
``HADOOP_USER_NAME`` and ``HADOOP_USER_PASSWORD``.
"""
import os
import warnings

import pandas as pd

DRIVERS = ("presto", "hive")


def _credentials():
    try:
        return os.environ["HADOOP_USER_NAME"], os.environ["HADOOP_USER_PASSWORD"]
    except KeyError as exc:
        raise RuntimeError(f"Environment variable {exc.args[0]} not found, please set it.")


class PrestoConnector:
    """Fast read-only SQL through a Presto coordinator."""

    def __init__(self, host, port=8080, catalog="hive", session_props=None, **_):
        import sqlalchemy as sa

        user, password = _credentials()
        url = f"presto://{user}#{password}@{host}:{port}/{catalog}"
        connect_args = {"session_props": session_props} if session_props else {}
        self._engine = sa.create_engine(url, connect_args=connect_args)

    def query(self, sql):
        with self._engine.connect() as conn:
            result = conn.exec_driver_sql(sql)
            return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


class HiveConnector:
    """SQL through HiveServer2 (slower than Presto, handles very large jobs)."""

    def __init__(self, host, port=10000, auth="CUSTOM", configuration=None, **_):
        from pyhive import hive

        user, password = _credentials()
        self._conn = hive.connect(
            host=host, port=int(port), auth=auth, username=user, password=password,
            configuration={k: str(v) for k, v in (configuration or {}).items()},
        )

    def query(self, sql):
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [c[0].split(".")[-1] for c in cursor.description]
        finally:
            cursor.close()
        return pd.DataFrame(rows, columns=columns)


def build_connector(driver, connections):
    """Create a connector from the agent's ``connections`` config."""
    if driver not in DRIVERS:
        raise ValueError(f"driver must be one of {DRIVERS}")
    settings = dict((connections or {}).get(driver) or {})
    if "host" not in settings:
        raise ValueError(f"connections.{driver}.host is missing in the agent config")
    cls = {"presto": PrestoConnector, "hive": HiveConnector}[driver]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return cls(**settings)
