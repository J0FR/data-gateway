import tempfile
import time
import unittest

import pandas as pd
from starlette.testclient import TestClient

from data_gateway.agent import AgentConfig, AgentService
from data_gateway.agent import create_app as create_agent_app
from data_gateway.server import Settings, create_app
from data_gateway.sqlguard import UnsafeSQL, check_read_only
from tests.test_gateway import ADMIN, FakeConnector, FakeNetwork

AUTH = ("admin", ADMIN)


class SqlGuardTests(unittest.TestCase):
    def test_allows_read_only(self):
        for sql in (
            "select 1", "SELECT * FROM t WHERE a = 'x;y' -- ; drop table t", "show tables",
            "describe db.t", "explain select 1", "with a as (select 1 x) select * from a;",
            "select 'insert into t' as note", 'select "drop" from t', "/* update */ select 1",
        ):
            check_read_only(sql)

    def test_strips_trailing_semicolon(self):
        self.assertEqual(check_read_only("  select 1 ;  "), "select 1")

    def test_blocks_writes_and_tricks(self):
        for sql in (
            "", "   ", "insert into t values (1)", "drop table t", "select 1; drop table t",
            "with a as (select 1) insert into t select * from a", "update t set a=1",
            "create table x as select 1", "set session foo=1", "select * from t into outfile 'x'",
            "select 'unterminated", "select /* never closed", "call proc()", "use other_db",
            "truncate table t", "select 1 from t; select 2", "x" * 20001,
        ):
            with self.subTest(sql=sql[:40]):
                with self.assertRaises(UnsafeSQL):
                    check_read_only(sql)


class SqlPageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.net = FakeNetwork()
        self.server = create_app(
            Settings(database_url=f"sqlite:///{self.tmp.name}/r.db", admin_token=ADMIN),
            http=self.net, start_scheduler=False)
        self.svc = self.server.state.ctx.services
        self.net.add("relay.test", self.server)
        self.api = TestClient(self.server)

    def tearDown(self):
        self.server.state.ctx.store.engine.dispose()
        self.tmp.cleanup()

    def add_agent(self, df, allow=True, cap=10000, name="pc1", host="pc1.test"):
        _, secret, token = self.svc.register_agent("t", name, f"http://{host}")
        cfg = AgentConfig(name=name, secret=secret, relay_url="http://relay.test", token=token,
                          allow_adhoc_sql=allow, adhoc_max_rows=cap)
        service = AgentService(cfg, connector_factory=lambda d: FakeConnector(df),
                               http=self.net, sleep=lambda s: None)
        self.net.add(host, create_agent_app(service))
        return service

    def run_sql(self, sql, driver="presto"):
        r = self.api.post("/admin/sql/run", json={"driver": driver, "sql": sql}, auth=AUTH)
        self.assertEqual(r.status_code, 200, r.text)
        qid = r.json()["id"]
        for _ in range(100):
            j = self.api.get(f"/admin/sql/{qid}", auth=AUTH).json()
            if j["status"] not in ("pending", "running"):
                return qid, j
            time.sleep(0.05)
        self.fail(f"query never finished: {j}")

    def test_query_runs_on_agent_and_returns_rows(self):
        self.add_agent(pd.DataFrame({"id": [1, 2], "city": ["Sao Paulo", "Rio"]}))
        qid, j = self.run_sql("select id, city from t")
        self.assertEqual(j["status"], "success", j)
        self.assertEqual(j["columns"], ["id", "city"])
        self.assertEqual(j["rows"], [[1, "Sao Paulo"], [2, "Rio"]])
        self.assertFalse(j["truncated"])
        csv = self.api.get(f"/admin/sql/{qid}/csv", auth=AUTH)
        self.assertEqual(csv.text.splitlines()[0], "id,city")
        self.assertIn("Sao Paulo", csv.text)

    def test_result_is_capped(self):
        self.add_agent(pd.DataFrame({"n": range(10)}), cap=3)
        _, j = self.run_sql("select n from t")
        self.assertEqual(len(j["rows"]), 3)
        self.assertTrue(j["truncated"])

    def test_csv_neutralises_formulas(self):
        self.add_agent(pd.DataFrame({"v": ["=HYPERLINK(1)", "ok"]}))
        qid, _ = self.run_sql("select v from t")
        self.assertIn("'=HYPERLINK(1)", self.api.get(f"/admin/sql/{qid}/csv", auth=AUTH).text)

    def test_write_statements_never_reach_an_agent(self):
        svc = self.add_agent(pd.DataFrame({"a": [1]}))
        for sql in ("drop table t", "select 1; delete from t", "insert into t values (1)"):
            r = self.api.post("/admin/sql/run", json={"driver": "hive", "sql": sql}, auth=AUTH)
            self.assertEqual(r.status_code, 400, sql)
        self.assertIsNone(svc.task("adhoc-1"))

    def test_agent_enforces_rules_even_if_the_server_is_bypassed(self):
        on = self.add_agent(pd.DataFrame({"a": [1]}))
        self.assertEqual(on.start_adhoc(9, "presto", "drop table t")[0], 400)
        self.assertEqual(on.start_adhoc(9, "clickhouse", "select 1")[0], 400)
        off = self.add_agent(pd.DataFrame({"a": [1]}), allow=False, name="pc2", host="pc2.test")
        self.assertEqual(off.start_adhoc(9, "presto", "select 1")[0], 403)

    def test_no_agent_with_adhoc_enabled_gives_clear_error(self):
        self.add_agent(pd.DataFrame({"a": [1]}), allow=False)
        _, j = self.run_sql("select 1")
        self.assertEqual(j["status"], "failed")
        self.assertIn("allow_adhoc_sql", j["error"])

    def test_agent_failure_is_shown(self):
        svc = self.add_agent(pd.DataFrame({"a": [1]}))

        class Boom:
            def query(self, sql, max_rows=None):
                raise RuntimeError("table not found: db.nope")

        svc._factory = lambda d: Boom()
        _, j = self.run_sql("select * from db.nope")
        self.assertEqual(j["status"], "failed")
        self.assertIn("table not found", j["error"])

    def test_page_static_files_auth_and_csrf(self):
        self.assertEqual(self.api.get("/admin/sql").status_code, 401)
        self.assertEqual(self.api.post("/admin/sql/run", json={"driver": "presto", "sql": "select 1"}).status_code, 401)
        page = self.api.get("/admin/sql", auth=AUTH)
        self.assertEqual(page.status_code, 200)
        self.assertIn("/static/cm/codemirror.min.js", page.text)
        for path in ("codemirror.min.js", "mode/sql.min.js", "addon/sql-hint.min.js",
                     "addon/show-hint.min.js", "codemirror.min.css"):
            self.assertEqual(self.api.get(f"/static/cm/{path}").status_code, 200, path)
        r = self.api.post("/admin/sql/run", json={"driver": "presto", "sql": "select 1"}, auth=AUTH,
                          headers={"Origin": "http://evil.example"})
        self.assertEqual(r.status_code, 403)

    def test_history_lists_recent_queries_escaped(self):
        self.add_agent(pd.DataFrame({"a": [1]}))
        self.run_sql("select '<script>alert(1)</script>' as x")
        page = self.api.get("/admin/sql", auth=AUTH).text
        self.assertNotIn("<script>alert(1)</script>", page)
        self.assertIn("&lt;script&gt;", page)


if __name__ == "__main__":
    unittest.main()
