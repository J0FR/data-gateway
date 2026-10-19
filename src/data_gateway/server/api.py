"""Relay HTTP API: agent uploads, dashboard refresh, background jobs."""
import logging
import os
import secrets
from dataclasses import dataclass

import sqlalchemy as sa
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles

from . import loader
from .db import Store
from .services import Services

log = logging.getLogger("data_gateway.server")
bearer = HTTPBearer(auto_error=False)


@dataclass
class Settings:
    database_url: str = "sqlite:///relay.db"
    admin_token: str = ""
    dashboard_token: str = ""       # lets the dashboards' refresh button call /api/refresh
    schema: str = None              # relay's own tables (default: "relay" on Postgres)
    data_schema: str = None         # where dataset tables are published
    max_upload_mb: int = 50
    job_expiry_minutes: int = 60
    health_interval: int = 30
    dispatch_interval: int = 5

    @classmethod
    def from_env(cls):
        e = os.environ
        return cls(
            database_url=e.get("RELAY_DATABASE_URL", cls.database_url),
            admin_token=e.get("RELAY_ADMIN_TOKEN", ""),
            dashboard_token=e.get("RELAY_DASHBOARD_TOKEN", ""),
            schema=e.get("RELAY_SCHEMA") or None,
            data_schema=e.get("RELAY_DATA_SCHEMA") or None,
            max_upload_mb=int(e.get("RELAY_MAX_UPLOAD_MB", 50)),
            job_expiry_minutes=int(e.get("RELAY_JOB_EXPIRY_MINUTES", 60)),
            health_interval=int(e.get("RELAY_HEALTH_INTERVAL", 30)),
            dispatch_interval=int(e.get("RELAY_DISPATCH_INTERVAL", 5)),
        )


class Context:
    """Everything the routes need; also handy for tests."""

    def __init__(self, settings, store, services):
        self.settings, self.store, self.services = settings, store, services


def create_app(settings=None, http=None, start_scheduler=True):
    settings = settings or Settings.from_env()
    if not settings.admin_token or len(settings.admin_token) < 12:
        raise RuntimeError("RELAY_ADMIN_TOKEN must be set (12+ characters)")
    store = Store(settings.database_url, settings.schema, settings.data_schema)
    store.create_all()
    services = Services(store, http=http, job_expiry_minutes=settings.job_expiry_minutes)
    ctx = Context(settings, store, services)

    app = FastAPI(title="data-gateway", docs_url=None, redoc_url=None)
    app.state.ctx = ctx
    # bundled CodeMirror (MIT) for the SQL test page; served locally so no internet is needed
    app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
              name="static")

    def is_admin(token):
        return bool(token) and secrets.compare_digest(token, settings.admin_token)

    def require_agent(creds: HTTPAuthorizationCredentials = Depends(bearer)):
        agent = services.agent_by_token(creds.credentials) if creds else None
        if not agent:
            raise HTTPException(401, "unknown or disabled agent token")
        return agent

    def require_refresher(creds: HTTPAuthorizationCredentials = Depends(bearer)):
        t = creds.credentials if creds else ""
        ok = is_admin(t) or (settings.dashboard_token and secrets.compare_digest(
            t, settings.dashboard_token))
        if not ok:
            raise HTTPException(401, "invalid token")
        return "admin" if is_admin(t) else "dashboard"

    def require_admin_api(creds: HTTPAuthorizationCredentials = Depends(bearer)):
        if not (creds and is_admin(creds.credentials)):
            raise HTTPException(401, "admin token required")

    @app.get("/healthz")
    def healthz():
        store.row(sa.select(sa.literal(1).label("ok")))
        return {"ok": True}

    @app.post("/datasets/{name}/upload")
    async def upload(name: str, request: Request, agent=Depends(require_agent),
                     x_relay_job: str = Header(default=None),
                     content_encoding: str = Header(default="")):
        cfg = services.get_dataset(name)
        if not cfg:
            raise HTTPException(404, "unknown dataset")
        if not services.agent_allows(agent, name):
            raise HTTPException(403, "this agent may not upload this dataset")
        job_id = int(x_relay_job) if x_relay_job and x_relay_job.isdigit() else None
        raw = await request.body()
        digest = None
        source = f"agent:{agent['name']}"
        try:
            payload = loader.decode_body(raw, "gzip" in content_encoding.lower(),
                                         settings.max_upload_mb * 1024 * 1024)
            digest = loader.sha256_hex(payload)
            df = loader.parse_upload(payload)
            version = loader.load_dataset(store, cfg, df, source, job_id, digest)
        except loader.ValidationError as exc:
            loader.record_rejection(store, name, source, job_id, digest or "", str(exc))
            if job_id:
                services.finish_job(job_id, "failed", f"rejected: {exc}")
            store.audit_log(source, "upload.rejected", f"{name}: {exc}")
            raise HTTPException(422, f"rejected: {exc}")
        if job_id:
            services.finish_job(job_id, "success", None, len(df))
        store.audit_log(source, "upload.ok", f"{name} v{version} rows={len(df)}")
        return {"dataset": name, "version": version, "rows": len(df)}

    @app.post("/agent/jobs/{job_id}/fail")
    async def agent_fail(job_id: int, request: Request, agent=Depends(require_agent)):
        msg = (await request.body()).decode("utf-8", "replace")[:500]
        j = store.jobs
        job = store.row(sa.select(j).where(j.c.id == job_id))
        if not job or job["agent_id"] != agent["id"]:
            raise HTTPException(404, "unknown job")
        services.fail_or_requeue(job, f"agent error: {msg}")
        return {"ok": True}

    @app.post("/api/refresh/{dataset}", status_code=202)
    def refresh(dataset: str, who: str = Depends(require_refresher)):
        try:
            job = services.enqueue_job(dataset, "refresh_button", who)
        except ValueError:
            raise HTTPException(404, "unknown dataset")
        return {"job_id": job["id"], "status": job["status"]}

    @app.get("/api/jobs/{job_id}")
    def job_status(job_id: int, who: str = Depends(require_refresher)):
        job = store.row(sa.select(store.jobs).where(store.jobs.c.id == job_id))
        if not job:
            raise HTTPException(404, "unknown job")
        return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in job.items()}

    @app.get("/api/status", dependencies=[Depends(require_admin_api)])
    def status():
        ds = services.dataset_status()
        return {
            "datasets": [{
                "name": d["name"], "stale": d["stale"],
                "last_update": d["last"]["loaded_at"].isoformat() if d["last"] else None,
                "source": d["last"]["source"] if d["last"] else None,
                "rows": d["last"]["row_count"] if d["last"] else None,
            } for d in ds],
        }

    from .admin import build_admin_router
    app.include_router(build_admin_router(ctx))

    if start_scheduler:
        sched = BackgroundScheduler(timezone="UTC")
        sched.add_job(services.dispatch_once, "interval", seconds=settings.dispatch_interval,
                      max_instances=1, coalesce=True)
        sched.add_job(services.health_check_all, "interval", seconds=settings.health_interval,
                      max_instances=1, coalesce=True)
        sched.add_job(services.reconcile_running, "interval", seconds=30,
                      max_instances=1, coalesce=True)
        sched.add_job(services.enqueue_due_schedules, "interval", seconds=30,
                      max_instances=1, coalesce=True)

        @app.on_event("startup")
        def _start():
            sched.start()

        @app.on_event("shutdown")
        def _stop():
            sched.shutdown(wait=False)

        app.state.scheduler = sched
    return app
