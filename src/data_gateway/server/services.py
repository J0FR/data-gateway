"""Agents, jobs, dispatching, health checks and schedules."""
import json
import logging
import secrets
from datetime import timedelta, timezone

import sqlalchemy as sa
from apscheduler.triggers.cron import CronTrigger

from ..common import HttpClient, sha256_hex, sign_headers, utcnow
from .loader import NAME_RE

log = logging.getLogger("data_gateway.server")

ACTIVE = ("pending", "running")
MAX_ATTEMPTS = 3


def hash_token(token):
    return sha256_hex(token.encode())


class Services:
    def __init__(self, store, http=None, job_expiry_minutes=60):
        self.store = store
        self.http = http or HttpClient()
        self.job_expiry = timedelta(minutes=job_expiry_minutes)

    # ===== datasets & schedules =====
    def upsert_dataset(self, actor, name, **fields):
        if not NAME_RE.match(name):
            raise ValueError("dataset name must be lowercase letters, digits, underscores")
        s, t = self.store, self.store.datasets
        if s.row(sa.select(t).where(t.c.name == name)):
            s.execute(sa.update(t).where(t.c.name == name).values(**fields))
        else:
            s.insert(t, name=name, **fields)
        s.audit_log(actor, "dataset.save", name)

    def get_dataset(self, name):
        t = self.store.datasets
        return self.store.row(sa.select(t).where(t.c.name == name))

    def set_schedule(self, actor, dataset, cron, enabled=True):
        CronTrigger.from_crontab(cron, timezone=timezone.utc)  # validates
        if not self.get_dataset(dataset):
            raise ValueError("unknown dataset")
        t, s = self.store.schedules, self.store
        if s.row(sa.select(t).where(t.c.dataset == dataset)):
            s.execute(sa.update(t).where(t.c.dataset == dataset).values(cron=cron, enabled=enabled))
        else:
            s.insert(t, dataset=dataset, cron=cron, enabled=enabled)
        s.audit_log(actor, "schedule.save", f"{dataset} {cron} enabled={enabled}")

    def toggle_schedule(self, actor, dataset, enabled):
        t = self.store.schedules
        self.store.execute(sa.update(t).where(t.c.dataset == dataset).values(enabled=enabled))
        self.store.audit_log(actor, "schedule.toggle", f"{dataset} enabled={enabled}")

    def enqueue_due_schedules(self, now=None):
        """Create a job for every enabled schedule whose time has come."""
        now = now or utcnow()
        t = self.store.schedules
        created = []
        for sch in self.store.rows(sa.select(t).where(t.c.enabled.is_(True))):
            base = (sch["last_run"] or (now - timedelta(minutes=1))).replace(tzinfo=timezone.utc)
            trigger = CronTrigger.from_crontab(sch["cron"], timezone=timezone.utc)
            nxt = trigger.get_next_fire_time(None, base)
            if nxt is not None and nxt.replace(tzinfo=None) <= now:
                job = self.enqueue_job(sch["dataset"], "schedule", "scheduler")
                self.store.execute(sa.update(t).where(t.c.dataset == sch["dataset"]).values(last_run=now))
                created.append(job)
        return created

    # ===== agents =====
    def register_agent(self, actor, name, url, datasets="*"):
        """Returns (agent_id, secret, token). Secret/token are shown only once."""
        secret, token = secrets.token_urlsafe(32), "rt_" + secrets.token_urlsafe(32)
        aid = self.store.insert(
            self.store.agents, name=name, url=url.rstrip("/"), secret=secret,
            token_hash=hash_token(token), enabled=True, status="unknown", busy=False,
            datasets=datasets or "*", created_at=utcnow(),
        )
        self.store.audit_log(actor, "agent.register", name)
        return aid, secret, token

    def rotate_token(self, actor, agent_id):
        secret, token = secrets.token_urlsafe(32), "rt_" + secrets.token_urlsafe(32)
        a = self.store.agents
        self.store.execute(sa.update(a).where(a.c.id == agent_id).values(
            secret=secret, token_hash=hash_token(token)))
        self.store.audit_log(actor, "agent.rotate", agent_id)
        return secret, token

    def set_agent_enabled(self, actor, agent_id, enabled):
        a = self.store.agents
        self.store.execute(sa.update(a).where(a.c.id == agent_id).values(
            enabled=enabled, status="unknown" if enabled else "disabled"))
        self.store.audit_log(actor, "agent.enable" if enabled else "agent.disable", agent_id)

    def agent_by_token(self, token):
        a = self.store.agents
        row = self.store.row(sa.select(a).where(a.c.token_hash == hash_token(token)))
        return row if row and row["enabled"] else None

    @staticmethod
    def agent_allows(agent, dataset):
        allowed = [x.strip() for x in (agent["datasets"] or "*").split(",") if x.strip()]
        return "*" in allowed or dataset in allowed

    def _call(self, agent, method, path, body=b""):
        headers = sign_headers(agent["secret"], method, path, body)
        url = agent["url"] + path
        if method == "GET":
            return self.http.get(url, headers=headers, timeout=5)
        headers["Content-Type"] = "application/json"
        return self.http.post(url, body=body, headers=headers, timeout=10)

    def ping(self, agent):
        """GET /alive; updates the agent row. Returns the agent's reply or None."""
        a = self.store.agents
        try:
            resp = self._call(agent, "GET", "/alive")
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            info = resp.json()
        except Exception as exc:
            log.info("agent %s offline: %s", agent["name"], exc)
            self.store.execute(sa.update(a).where(a.c.id == agent["id"]).values(
                status="offline", busy=False))
            return None
        self.store.execute(sa.update(a).where(a.c.id == agent["id"]).values(
            status="online", busy=bool(info.get("busy")), version=info.get("version"),
            last_seen=utcnow()))
        return info

    def health_check_all(self):
        a = self.store.agents
        for agent in self.store.rows(sa.select(a).where(a.c.enabled.is_(True))):
            self.ping(agent)

    # ===== jobs =====
    def enqueue_job(self, dataset, trigger, requested_by):
        """Create a pending job, or return the one already queued/running."""
        if not self.get_dataset(dataset):
            raise ValueError("unknown dataset")
        j = self.store.jobs
        existing = self.store.row(
            sa.select(j).where(j.c.dataset == dataset, j.c.status.in_(ACTIVE)).order_by(j.c.id))
        if existing:
            return existing
        jid = self.store.insert(j, dataset=dataset, trigger=trigger, requested_by=requested_by,
                                status="pending", attempts=0, created_at=utcnow())
        self.store.audit_log(requested_by, "job.create", f"{jid} {dataset} via {trigger}")
        return self.store.row(sa.select(j).where(j.c.id == jid))

    def finish_job(self, job_id, status, error=None, rows=None):
        j = self.store.jobs
        self.store.execute(sa.update(j).where(j.c.id == job_id, j.c.status.in_(ACTIVE)).values(
            status=status, error=error, rows=rows, finished_at=utcnow()))

    def fail_or_requeue(self, job, error):
        j = self.store.jobs
        current = self.store.row(sa.select(j).where(j.c.id == job["id"]))
        if not current or current["status"] != "running":
            return  # already finished (e.g. the relay rejected the data): never revive it
        if job["attempts"] < MAX_ATTEMPTS:
            self.store.execute(sa.update(j).where(j.c.id == job["id"], j.c.status == "running").values(
                status="pending", agent_id=None, started_at=None, error=error))
        else:
            self.finish_job(job["id"], "failed", error)

    def cancel_job(self, actor, job_id):
        j = self.store.jobs
        job = self.store.row(sa.select(j).where(j.c.id == job_id))
        if not job or job["status"] not in ACTIVE:
            return False
        if job["status"] == "running" and job["agent_id"]:
            a = self.store.agents
            agent = self.store.row(sa.select(a).where(a.c.id == job["agent_id"]))
            try:
                if agent:
                    self._call(agent, "POST", f"/task/job-{job_id}/cancel")
            except Exception:
                pass
        self.finish_job(job_id, "cancelled", f"cancelled by {actor}")
        self.store.audit_log(actor, "job.cancel", job_id)
        return True

    def retry_job(self, actor, job_id):
        j = self.store.jobs
        job = self.store.row(sa.select(j).where(j.c.id == job_id))
        if not job or job["status"] not in ("failed", "expired", "cancelled"):
            return None
        return self.enqueue_job(job["dataset"], "retry", actor)

    def dispatch_once(self):
        """Give each pending job to an idle agent (ping first, then send the task)."""
        j, a = self.store.jobs, self.store.agents
        now = utcnow()
        for job in self.store.rows(sa.select(j).where(j.c.status == "pending").order_by(j.c.id)):
            if now - job["created_at"] > self.job_expiry:
                self.finish_job(job["id"], "expired", "no agent was available in time")
                continue
            agents = self.store.rows(sa.select(a).where(a.c.enabled.is_(True)).order_by(
                a.c.last_seen.desc()))
            for agent in agents:
                if not self.agent_allows(agent, job["dataset"]):
                    continue
                info = self.ping(agent)
                if info is None or info.get("busy"):
                    continue
                body = json.dumps({"job_id": job["id"], "dataset": job["dataset"]}).encode()
                try:
                    resp = self._call(agent, "POST", "/task", body)
                except Exception as exc:
                    log.warning("send to %s failed: %s", agent["name"], exc)
                    continue
                if resp.status_code == 202:
                    self.store.execute(sa.update(j).where(j.c.id == job["id"]).values(
                        status="running", agent_id=agent["id"], started_at=utcnow(),
                        attempts=job["attempts"] + 1, error=None))
                    self.store.execute(sa.update(a).where(a.c.id == agent["id"]).values(busy=True))
                    break

    def reconcile_running(self):
        """Time out stuck jobs; requeue jobs whose agent forgot them (e.g. PC restarted)."""
        j, a = self.store.jobs, self.store.agents
        now = utcnow()
        for job in self.store.rows(sa.select(j).where(j.c.status == "running")):
            ds = self.get_dataset(job["dataset"]) or {}
            limit = timedelta(minutes=ds.get("max_duration_minutes") or 30)
            if job["started_at"] and now - job["started_at"] > limit:
                self.fail_or_requeue(job, "timed out")
                continue
            agent = self.store.row(sa.select(a).where(a.c.id == job["agent_id"]))
            if not agent:
                self.fail_or_requeue(job, "agent was removed")
                continue
            try:
                resp = self._call(agent, "GET", f"/task/job-{job['id']}")
            except Exception:
                continue  # unreachable now; the timeout above will catch a dead agent
            if resp.status_code == 404:
                self.fail_or_requeue(job, "agent lost the task (restarted?)")

    # ===== status for dashboards =====
    def dataset_status(self, now=None):
        now = now or utcnow()
        v = self.store.versions
        out = []
        for ds in self.store.rows(sa.select(self.store.datasets).order_by(self.store.datasets.c.name)):
            last = self.store.row(sa.select(v).where(
                v.c.dataset == ds["name"], v.c.status == "live").order_by(v.c.version.desc()))
            stale = (last is None) or (
                now - last["loaded_at"] > timedelta(minutes=ds["stale_after_minutes"]))
            out.append({**ds, "last": last, "stale": stale})
        return out
