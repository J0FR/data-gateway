import base64
import os
import tempfile
import time
import unittest

import pandas as pd
import sqlalchemy as sa
from starlette.testclient import TestClient

from data_gateway.agent import AgentConfig, AgentService
from data_gateway.agent import create_app as create_agent_app
from data_gateway.common import SignatureError, Verifier, sign_headers
from data_gateway.server import Settings, create_app
from data_gateway.server import loader

ADMIN = "admin-token-123456"


class FakeNetwork:
    """Routes requests by host to in-process apps (no real sockets)."""

    def __init__(self):
        self.apps = {}

    def add(self, host, app):
        self.apps[host] = TestClient(app, base_url=f"http://{host}")

    def _client(self, url):
        host = url.split("//", 1)[1].split("/", 1)[0]
        if host not in self.apps:
            raise ConnectionError(f"{host} unreachable")
        return self.apps[host]

    def get(self, url, headers=None, timeout=5):
        return self._client(url).get(url, headers=headers)

    def post(self, url, body=b"", headers=None, timeout=10):
        return self._client(url).post(url, content=body, headers=headers)


class FakeConnector:
    def __init__(self, df):
        self.df = df

    def query(self, sql):
        return self.df


class SigningTests(unittest.TestCase):
    def test_roundtrip_and_replay(self):
        v = Verifier("s3cret")
        h = sign_headers("s3cret", "POST", "/task", b"{}")
        v.verify("POST", "/task", b"{}", h)
        with self.assertRaises(SignatureError):
            v.verify("POST", "/task", b"{}", h)  # replay

    def test_bad_secret_stale_and_tamper(self):
        v = Verifier("s3cret")
        with self.assertRaises(SignatureError):
            v.verify("GET", "/alive", b"", sign_headers("wrong", "GET", "/alive"))
        with self.assertRaises(SignatureError):
            v.verify("GET", "/alive", b"", sign_headers("s3cret", "GET", "/alive", now=time.time() - 600))
        with self.assertRaises(SignatureError):
            v.verify("POST", "/task", b"x", sign_headers("s3cret", "POST", "/task", b"y"))


class LoaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        app = create_app(Settings(database_url=f"sqlite:///{self.tmp.name}/r.db", admin_token=ADMIN),
                         start_scheduler=False)
        self.store = app.state.ctx.store
        self.cfg = {"name": "sales", "key_columns": "id", "min_rows": 1, "max_change_pct": 50,
                    "allow_schema_change": False}

    def tearDown(self):
        self.store.engine.dispose()
        self.tmp.cleanup()

    def live(self):
        with self.store.engine.begin() as c:
            return c.execute(sa.text("select id, amount from sales order by id")).fetchall()

    def test_good_load_then_bad_loads_keep_live_data(self):
        good = pd.DataFrame({"id": [1, 2, 3], "amount": [1.0, 2.0, 3.0]})
        self.assertEqual(loader.load_dataset(self.store, self.cfg, good, "t"), 1)
        for bad in (
            pd.DataFrame({"id": [], "amount": []}),                          # empty
            pd.DataFrame({"id": [1, 1, 2], "amount": [1.0, 2.0, 3.0]}),      # duplicate key
            pd.DataFrame({"id": [1, None, 3], "amount": [1.0, 2.0, 3.0]}),   # null key
            pd.DataFrame({"id": [1, 2, 3], "other": [1, 2, 3]}),             # schema change
            pd.DataFrame({"id": [1], "amount": [1.0]}),                      # row count drop > 50%
        ):
            with self.assertRaises(loader.ValidationError):
                loader.load_dataset(self.store, self.cfg, bad, "t")
        self.assertEqual(self.live(), [(1, 1.0), (2, 2.0), (3, 3.0)])

    def test_second_load_replaces_and_rollback_restores(self):
        a = pd.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]})
        b = pd.DataFrame({"id": [1, 2, 3], "amount": [9.0, 8.0, 7.0]})
        loader.load_dataset(self.store, self.cfg, a, "t")
        self.assertEqual(loader.load_dataset(self.store, self.cfg, b, "t"), 2)
        self.assertEqual(len(self.live()), 3)
        loader.rollback_dataset(self.store, "sales", "me")
        self.assertEqual(self.live(), [(1, 1.0), (2, 2.0)])

    def test_gzip_limit(self):
        with self.assertRaises(loader.ValidationError):
            loader.decode_body(b"x" * 100, False, 10)


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.net = FakeNetwork()
        self.server = create_app(
            Settings(database_url=f"sqlite:///{self.tmp.name}/r.db", admin_token=ADMIN,
                     dashboard_token="dash-token-1234"),
            http=self.net, start_scheduler=False)
        self.ctx = self.server.state.ctx
        self.svc = self.ctx.services
        self.net.add("relay.test", self.server)
        self.api = TestClient(self.server)
        self.svc.upsert_dataset("t", "sales", key_columns="id", stale_after_minutes=60)

    def tearDown(self):
        self.ctx.store.engine.dispose()
        self.tmp.cleanup()

    def add_agent(self, df, name="pc1", host="pc1.test", datasets="*"):
        _, secret, token = self.svc.register_agent("t", name, f"http://{host}", datasets)
        cfg = AgentConfig(name=name, secret=secret, relay_url="http://relay.test", token=token,
                          datasets={"sales": {"driver": "presto", "sql": "select 1"}},
                          upload_retries=1)
        service = AgentService(cfg, connector_factory=lambda d: FakeConnector(df),
                               http=self.net, sleep=lambda s: None)
        self.net.add(host, create_agent_app(service))
        return service

    def wait_job(self, job_id, status):
        for _ in range(100):
            j = self.ctx.store.row(sa.select(self.ctx.store.jobs).where(
                self.ctx.store.jobs.c.id == job_id))
            if j["status"] == status:
                return j
            time.sleep(0.05)
        self.fail(f"job {job_id} never reached {status}: {j}")

    def live_count(self):
        with self.ctx.store.engine.begin() as c:
            return c.execute(sa.text("select count(*) from sales")).scalar()

    def test_refresh_button_flows_through_agent_to_database(self):
        self.add_agent(pd.DataFrame({"id": [1, 2, 3], "amount": [1.0, 2.0, 3.0]}))
        r = self.api.post("/api/refresh/sales", headers={"Authorization": "Bearer dash-token-1234"})
        self.assertEqual(r.status_code, 202)
        job_id = r.json()["job_id"]
        self.svc.dispatch_once()
        self.wait_job(job_id, "success")
        self.assertEqual(self.live_count(), 3)
        status = self.api.get("/api/status", headers={"Authorization": f"Bearer {ADMIN}"}).json()
        self.assertEqual(status["datasets"][0]["source"], "agent:pc1")
        self.assertFalse(status["datasets"][0]["stale"])

    def test_no_agent_online_keeps_job_pending_and_dashboard_says_so(self):
        self.svc.register_agent("t", "ghost", "http://ghost.test")
        job = self.svc.enqueue_job("sales", "refresh_button", "dashboard")
        self.svc.dispatch_once()
        self.assertEqual(self.ctx.store.row(sa.select(self.ctx.store.jobs))["status"], "pending")
        self.assertEqual(job["status"], "pending")
        page = self.api.get("/admin", auth=("admin", ADMIN)).text
        self.assertIn("No computer is online", page)
        self.assertIn("OLD", page)

    def test_duplicate_refresh_reuses_active_job(self):
        a = self.svc.enqueue_job("sales", "refresh_button", "x")
        b = self.svc.enqueue_job("sales", "schedule", "scheduler")
        self.assertEqual(a["id"], b["id"])

    def test_empty_query_fails_job_and_keeps_old_data(self):
        self.add_agent(pd.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]}), name="good", host="good.test")
        job = self.svc.enqueue_job("sales", "t", "t")
        self.svc.dispatch_once()
        self.wait_job(job["id"], "success")
        # replace with an agent that returns nothing
        self.ctx.store.execute(sa.update(self.ctx.store.agents).values(enabled=False))
        self.add_agent(pd.DataFrame({"id": [], "amount": []}), name="empty", host="empty.test")
        job = self.svc.enqueue_job("sales", "t", "t")
        for _ in range(4):  # attempts are retried, then it fails for good
            self.svc.dispatch_once()
            time.sleep(0.3)
        j = self.wait_job(job["id"], "failed")
        self.assertIn("no rows", j["error"])
        self.assertEqual(self.live_count(), 2)

    def test_rejected_upload_is_recorded_and_live_data_kept(self):
        self.add_agent(pd.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]}))
        job = self.svc.enqueue_job("sales", "t", "t")
        self.svc.dispatch_once()
        self.wait_job(job["id"], "success")
        # a second agent whose query returns the wrong columns
        self.add_agent(pd.DataFrame({"id": [1, 2], "wrong": [1, 2]}), name="pc2", host="pc2.test")
        self.ctx.store.execute(sa.update(self.ctx.store.agents).where(
            self.ctx.store.agents.c.name == "pc1").values(enabled=False))
        job = self.svc.enqueue_job("sales", "t", "t")
        self.svc.dispatch_once()
        j = self.wait_job(job["id"], "failed")
        self.assertIn("rejected", j["error"])
        self.assertEqual(self.live_count(), 2)
        v = self.ctx.store.rows(sa.select(self.ctx.store.versions))
        self.assertIn("rejected", [x["status"] for x in v])

    def test_auth(self):
        self.assertEqual(self.api.post("/datasets/sales/upload", content=b"{}").status_code, 401)
        self.assertEqual(self.api.post("/datasets/sales/upload", content=b"{}",
                                       headers={"Authorization": "Bearer nope"}).status_code, 401)
        self.assertEqual(self.api.post("/api/refresh/sales").status_code, 401)
        self.assertEqual(self.api.get("/admin").status_code, 401)
        self.assertEqual(self.api.get("/api/status").status_code, 401)
        # agent enforces signatures too
        self.add_agent(pd.DataFrame({"id": [1], "amount": [1.0]}))
        self.assertEqual(self.net.get("http://pc1.test/alive").status_code, 401)

    def test_agent_only_runs_allowed_datasets_and_one_at_a_time(self):
        svc = self.add_agent(pd.DataFrame({"id": [1], "amount": [1.0]}))
        self.assertEqual(svc.start(1, "unknown")[0], 404)

    def test_admin_actions_and_csrf(self):
        auth = ("admin", ADMIN)
        r = self.api.post("/admin/agents", data={"name": "pcx", "url": "http://x:1", "datasets": "*"},
                          auth=auth)
        self.assertEqual(r.status_code, 200)
        self.assertIn("shown only once", r.text)
        r = self.api.post("/admin/agents", data={"name": "pcy", "url": "http://x:1"}, auth=auth,
                          headers={"Origin": "http://evil.example"})
        self.assertEqual(r.status_code, 403)
        r = self.api.post("/admin/datasets", data={"name": "other", "cron": "0 6 * * *"}, auth=auth,
                          follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertIn("other", self.api.get("/admin", auth=auth).text)

    def test_schedule_enqueues_once(self):
        from datetime import timedelta

        self.svc.set_schedule("t", "sales", "* * * * *")
        from data_gateway.common import utcnow

        first = self.svc.enqueue_due_schedules(utcnow() + timedelta(minutes=2))
        self.assertEqual(len(first), 1)
        # an active job already exists, so the next tick does not pile up another
        again = self.svc.enqueue_due_schedules(utcnow() + timedelta(minutes=3))
        self.assertEqual(again[0]["id"], first[0]["id"])


if __name__ == "__main__":
    unittest.main()
