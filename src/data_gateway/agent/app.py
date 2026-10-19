"""Passive agent for a company PC.

The agent never asks the server for work. It only answers:

* ``GET  /alive``            - health: idle/busy, version
* ``POST /task``             - start a task ({"job_id", "dataset"}); 202, runs in background
* ``GET  /task/{id}``        - task status
* ``POST /task/{id}/cancel`` - cancel (result will be discarded)

Every request must be signed with the shared secret (see ``common.sign_headers``).
The agent only runs datasets listed in its local config, so the network can never
inject SQL.
"""
import gzip
import logging
import os
import threading
import time
from dataclasses import dataclass, field

import requests
import yaml
from fastapi import Depends, FastAPI, HTTPException, Request

from ..common import HttpClient, SignatureError, Verifier
from ..connectors import DRIVERS, build_connector

log = logging.getLogger("data_gateway.agent")

VERSION = "0.1.0"


@dataclass
class AgentConfig:
    name: str
    secret: str
    relay_url: str
    token: str
    datasets: dict = field(default_factory=dict)  # name -> {"driver": ..., "sql": ...}
    upload_retries: int = 3
    upload_timeout: int = 300
    connections: dict = field(default_factory=dict)  # driver -> host/port/... (see connectors.py)


def load_config(path):
    """Load agent YAML; environment variables override secrets."""
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    cfg = AgentConfig(
        name=raw["name"],
        secret=os.environ.get("RELAY_AGENT_SECRET", raw.get("secret", "")),
        relay_url=raw["relay_url"].rstrip("/"),
        token=os.environ.get("RELAY_AGENT_TOKEN", raw.get("token", "")),
        datasets=raw.get("datasets", {}),
        upload_retries=raw.get("upload_retries", 3),
        upload_timeout=raw.get("upload_timeout", 300),
        connections=raw.get("connections", {}),
    )
    if not cfg.secret or not cfg.token:
        raise ValueError("agent secret and token are required (config or env)")
    for name, d in cfg.datasets.items():
        if d.get("driver") not in DRIVERS or not d.get("sql"):
            raise ValueError(f"dataset {name!r} needs driver in {DRIVERS} and sql")
    return cfg


class AgentService:
    """Runs one task at a time and uploads the result to the relay."""

    def __init__(self, config, connector_factory=None, http=None, sleep=time.sleep):
        self.config = config
        self._factory = connector_factory or (
            lambda driver: build_connector(driver, self.config.connections))
        self._http = http or HttpClient()
        self._sleep = sleep
        self._lock = threading.Lock()
        self._tasks = {}
        self._current = None

    # ----- status -----
    def alive(self):
        with self._lock:
            return {
                "name": self.config.name,
                "version": VERSION,
                "busy": self._current is not None,
                "current_task": self._current,
                "datasets": sorted(self.config.datasets),
            }

    def task(self, task_id):
        with self._lock:
            return dict(self._tasks[task_id]) if task_id in self._tasks else None

    # ----- control -----
    def start(self, job_id, dataset, background=True):
        """Returns (http_status, body)."""
        if dataset not in self.config.datasets:
            return 404, {"error": f"dataset {dataset!r} is not allowed on this agent"}
        task_id = f"job-{job_id}"
        with self._lock:
            if self._current is not None and self._current != task_id:
                return 409, {"error": "agent busy", "current_task": self._current}
            if task_id in self._tasks and self._tasks[task_id]["state"] == "running":
                return 202, {"task_id": task_id}  # idempotent re-send
            self._tasks[task_id] = {
                "task_id": task_id, "job_id": job_id, "dataset": dataset,
                "state": "running", "error": None, "rows": None,
            }
            self._current = task_id
        if background:
            threading.Thread(target=self._run, args=(task_id,), daemon=True).start()
        else:
            self._run(task_id)
        return 202, {"task_id": task_id}

    def cancel(self, task_id):
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return False
            if t["state"] == "running":
                t["state"] = "cancelled"
            return True

    # ----- work -----
    def _finish(self, task_id, state, error=None, rows=None):
        with self._lock:
            t = self._tasks[task_id]
            if t["state"] != "cancelled":
                t["state"] = state
            t["error"], t["rows"] = error, rows
            self._current = None

    def _cancelled(self, task_id):
        with self._lock:
            return self._tasks[task_id]["state"] == "cancelled"

    def _run(self, task_id):
        t = self.task(task_id)
        job_id, dataset = t["job_id"], t["dataset"]
        spec = self.config.datasets[dataset]
        try:
            df = self._factory(spec["driver"]).query(spec["sql"])
            if self._cancelled(task_id):
                return self._finish(task_id, "cancelled")
            if df is None or df.empty:
                raise RuntimeError("query returned no rows; refusing to upload")
            self._upload(job_id, dataset, df)
            self._finish(task_id, "done", rows=len(df))
        except Exception as exc:  # report every failure to the relay
            log.exception("task %s failed", task_id)
            self._finish(task_id, "failed", error=str(exc)[:500])
            self._report_failure(job_id, str(exc)[:500])

    def _headers(self, job_id, extra=None):
        h = {"Authorization": f"Bearer {self.config.token}", "X-Relay-Job": str(job_id)}
        h.update(extra or {})
        return h

    def _upload(self, job_id, dataset, df):
        body = gzip.compress(df.to_json(orient="split", date_format="iso", index=False).encode())
        url = f"{self.config.relay_url}/datasets/{dataset}/upload"
        headers = self._headers(
            job_id, {"Content-Type": "application/json", "Content-Encoding": "gzip"}
        )
        last = None
        for attempt in range(self.config.upload_retries):
            try:
                resp = self._http.post(url, body=body, headers=headers,
                                       timeout=self.config.upload_timeout)
                if resp.status_code < 300:
                    return
                if resp.status_code < 500:  # relay rejected the data: retrying will not help
                    raise RuntimeError(f"relay rejected upload ({resp.status_code}): {resp.text[:300]}")
                last = f"relay error {resp.status_code}"
            except (requests.RequestException, OSError) as exc:
                last = str(exc)
            self._sleep(2 ** attempt)
        raise RuntimeError(f"upload failed after retries: {last}")

    def _report_failure(self, job_id, message):
        try:
            self._http.post(
                f"{self.config.relay_url}/agent/jobs/{job_id}/fail",
                body=message.encode(), headers=self._headers(job_id, {"Content-Type": "text/plain"}),
                timeout=10,
            )
        except Exception:
            log.warning("could not report failure of job %s to relay", job_id)


def create_app(service):
    app = FastAPI(title="data-gateway agent", docs_url=None, redoc_url=None)
    verifier = Verifier(service.config.secret)

    async def authenticated(request: Request):
        body = await request.body()
        try:
            verifier.verify(request.method, request.url.path, body, request.headers)
        except SignatureError as exc:
            raise HTTPException(401, str(exc))
        return body

    @app.get("/alive")
    async def alive(_=Depends(authenticated)):
        return service.alive()

    @app.post("/task", status_code=202)
    async def post_task(request: Request, body: bytes = Depends(authenticated)):
        import json

        try:
            data = json.loads(body or b"{}")
            job_id, dataset = int(data["job_id"]), str(data["dataset"])
        except (ValueError, KeyError, TypeError):
            raise HTTPException(400, "body must be JSON with job_id and dataset")
        status, payload = service.start(job_id, dataset)
        if status != 202:
            raise HTTPException(status, payload["error"])
        return payload

    @app.get("/task/{task_id}")
    async def get_task(task_id: str, _=Depends(authenticated)):
        t = service.task(task_id)
        if t is None:
            raise HTTPException(404, "unknown task")
        return t

    @app.post("/task/{task_id}/cancel")
    async def cancel(task_id: str, _=Depends(authenticated)):
        if not service.cancel(task_id):
            raise HTTPException(404, "unknown task")
        return {"task_id": task_id, "state": "cancelled"}

    return app
