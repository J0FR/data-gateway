"""Management dashboard (server-rendered HTML, auto-refreshing every 10s)."""
import html
import secrets
from urllib.parse import urlparse

import sqlalchemy as sa
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from ..common import utcnow
from . import loader
from .services import ACTIVE

esc = html.escape
basic = HTTPBasic(realm="data-gateway admin")

CSS = """
body{font:15px/1.45 system-ui,sans-serif;margin:0;background:#f5f6f8;color:#1c2330}
header{background:#1c2330;color:#fff;padding:14px 24px}header h1{margin:0;font-size:18px}
main{padding:18px 24px;max-width:1200px;margin:auto}
section{background:#fff;border:1px solid #dde1e8;border-radius:8px;padding:14px 18px;margin-bottom:18px}
h2{margin:0 0 10px;font-size:16px}table{border-collapse:collapse;width:100%}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #eceff3;font-size:14px;vertical-align:top}
th{color:#5b6677;font-weight:600}.ok{color:#117a3d}.warn{color:#9a6700}.bad{color:#b42318}
.pill{padding:1px 8px;border-radius:10px;font-size:12px;color:#fff;background:#6b7686}
.p-online,.p-success,.p-live{background:#117a3d}.p-offline,.p-failed,.p-rejected{background:#b42318}
.p-busy,.p-running{background:#2563eb}.p-pending,.p-unknown{background:#9a6700}
.summary{font-size:16px}.summary li{margin:4px 0}form.inline{display:inline}
button{cursor:pointer;border:1px solid #c5cbd5;background:#fff;border-radius:5px;padding:3px 9px}
input,select{padding:4px 6px;border:1px solid #c5cbd5;border-radius:5px}
.msg{background:#fff7e0;border:1px solid #f0d98a;padding:8px 12px;border-radius:6px;margin-bottom:14px}
code{background:#eef1f5;padding:1px 5px;border-radius:4px;word-break:break-all}
"""


def ago(dt, now):
    if not dt:
        return "never"
    s = int((now - dt).total_seconds())
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60} min ago"
    if s < 172800:
        return f"{s // 3600} h ago"
    return f"{s // 86400} days ago"


def pill(text):
    return f'<span class="pill p-{esc(str(text))}">{esc(str(text))}</span>'


def build_admin_router(ctx):
    store, svc, settings = ctx.store, ctx.services, ctx.settings
    router = APIRouter(prefix="/admin")

    def admin(creds: HTTPBasicCredentials = Depends(basic)):
        ok = secrets.compare_digest(creds.password.encode(), settings.admin_token.encode())
        if not ok:
            raise HTTPException(401, "bad credentials", headers={"WWW-Authenticate": "Basic"})
        return creds.username or "admin"

    def same_origin(request: Request):
        """Basic auth is sent automatically by browsers, so block cross-site POSTs."""
        origin = request.headers.get("origin") or request.headers.get("referer")
        if origin and urlparse(origin).netloc != request.headers.get("host"):
            raise HTTPException(403, "cross-site request blocked")

    def done(msg=""):
        return RedirectResponse(f"/admin?msg={esc(msg)}" if msg else "/admin", status_code=303)

    def page(body, refresh=True):
        meta = '<meta http-equiv="refresh" content="10">' if refresh else ""
        return HTMLResponse(
            f'<!doctype html><html><head><meta charset="utf-8">{meta}'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">'
            f"<title>Relay admin</title><style>{CSS}</style></head><body>"
            f'<header><h1>data-gateway · management</h1></header><main>{body}</main></body></html>'
        )

    def summary(now, agents, jobs_active, failed24, datasets):
        online = [a for a in agents if a["enabled"] and a["status"] == "online"]
        busy = [a for a in online if a["busy"]]
        running = sum(1 for j in jobs_active if j["status"] == "running")
        pending = sum(1 for j in jobs_active if j["status"] == "pending")
        stale = [d["name"] for d in datasets if d["stale"]]
        lines = ["<b>The service is running.</b>"]
        if not agents:
            lines.append('<span class="bad">No computers are registered yet, so nothing can fetch data.</span>')
        elif not online:
            lines.append('<span class="bad">No computer is online. Dashboards keep showing the last data, '
                         "but new refreshes will wait.</span>")
        else:
            lines.append(f'<span class="ok">{len(online)} of {len(agents)} registered computers are online'
                         f"{f' ({len(busy)} busy)' if busy else ''}.</span>")
        lines.append(f"{running} job(s) running, {pending} waiting. "
                     + (f'<span class="bad">{failed24} failed in the last 24h.</span>' if failed24
                        else "No failures in the last 24h."))
        if not datasets:
            lines.append("No datasets are set up yet.")
        elif stale:
            lines.append(f'<span class="warn">Data is out of date for: {esc(", ".join(stale))}.</span>')
        else:
            lines.append(f'<span class="ok">All {len(datasets)} datasets are up to date.</span>')
        return "<ul class='summary'>" + "".join(f"<li>{x}</li>" for x in lines) + "</ul>"

    @router.get("", response_class=HTMLResponse)
    def overview(msg: str = "", user: str = Depends(admin)):
        now = utcnow()
        a, j, v = store.agents, store.jobs, store.versions
        agents = store.rows(sa.select(a).order_by(a.c.name))
        jobs_active = store.rows(sa.select(j).where(j.c.status.in_(ACTIVE)).order_by(j.c.id))
        recent = store.rows(sa.select(j).order_by(j.c.id.desc()).limit(25))
        failed24 = sum(1 for x in recent if x["status"] in ("failed", "expired")
                       and x["created_at"] and (now - x["created_at"]).total_seconds() < 86400)
        datasets = svc.dataset_status(now)
        schedules = {s["dataset"]: s for s in store.rows(sa.select(store.schedules))}
        agent_names = {x["id"]: x["name"] for x in agents}

        out = [f'<div class="msg">{esc(msg)}</div>'] if msg else []
        out.append(f"<section><h2>Current state</h2>{summary(now, agents, jobs_active, failed24, datasets)}</section>")

        # computers
        rows = []
        for x in agents:
            state = "disabled" if not x["enabled"] else ("busy" if x["status"] == "online" and x["busy"] else x["status"])
            rows.append(
                f"<tr><td>{esc(x['name'])}</td><td>{pill(state)}</td><td>{esc(x['url'])}</td>"
                f"<td>{ago(x['last_seen'], now)}</td><td>{esc(x['version'] or '-')}</td>"
                f"<td>{esc(x['datasets'])}</td><td>"
                f"<form class='inline' method='post' action='/admin/agents/{x['id']}/{'disable' if x['enabled'] else 'enable'}'>"
                f"<button>{'Disable' if x['enabled'] else 'Enable'}</button></form> "
                f"<form class='inline' method='post' action='/admin/agents/{x['id']}/rotate' "
                f"onsubmit=\"return confirm('Issue new secret and token? The agent config must be updated.')\">"
                f"<button>Rotate secret</button></form></td></tr>")
        out.append(
            "<section><h2>Computers (agents)</h2><table><tr><th>Name</th><th>Status</th><th>Address</th>"
            "<th>Last seen</th><th>Version</th><th>Datasets allowed</th><th></th></tr>"
            + ("".join(rows) or "<tr><td colspan=7>None registered.</td></tr>") + "</table>"
            "<form method='post' action='/admin/agents' style='margin-top:10px'>"
            "Register: <input name='name' placeholder='name' required> "
            "<input name='url' placeholder='http://10.0.0.5:8765' size=26 required> "
            "<input name='datasets' placeholder='datasets (* = all)' value='*'> <button>Add</button></form></section>")

        # jobs
        jrows = []
        for x in recent:
            ctl = ""
            if x["status"] in ACTIVE:
                ctl = f"<form class='inline' method='post' action='/admin/jobs/{x['id']}/cancel'><button>Cancel</button></form>"
            elif x["status"] in ("failed", "expired", "cancelled"):
                ctl = f"<form class='inline' method='post' action='/admin/jobs/{x['id']}/retry'><button>Retry</button></form>"
            dur = ""
            if x["started_at"]:
                dur = f"{int(((x['finished_at'] or now) - x['started_at']).total_seconds())}s"
            jrows.append(
                f"<tr><td>{x['id']}</td><td>{esc(x['dataset'])}</td><td>{pill(x['status'])}</td>"
                f"<td>{esc(x['trigger'])}</td><td>{esc(agent_names.get(x['agent_id'], '-'))}</td>"
                f"<td>{ago(x['created_at'], now)}</td><td>{dur}</td><td>{esc(x['error'] or '')}</td><td>{ctl}</td></tr>")
        out.append(
            "<section><h2>Jobs (latest 25)</h2><table><tr><th>#</th><th>Dataset</th><th>Status</th><th>Trigger</th>"
            "<th>Computer</th><th>Created</th><th>Duration</th><th>Message</th><th></th></tr>"
            + ("".join(jrows) or "<tr><td colspan=9>No jobs yet.</td></tr>") + "</table></section>")

        # datasets
        drows = []
        for d in datasets:
            last = d["last"]
            s = schedules.get(d["name"])
            sched = (f"{esc(s['cron'])} " + ("(on)" if s["enabled"] else "(paused)")) if s else "-"
            tog = ""
            if s:
                tog = (f"<form class='inline' method='post' action='/admin/schedules/{esc(d['name'])}/"
                       f"{'pause' if s['enabled'] else 'resume'}'><button>{'Pause' if s['enabled'] else 'Resume'}</button></form>")
            drows.append(
                f"<tr><td>{esc(d['name'])}</td>"
                f"<td>{('<span class=bad>OLD</span> ' if d['stale'] else '<span class=ok>fresh</span> ')}"
                f"{ago(last['loaded_at'], now) if last else 'no data yet'}</td>"
                f"<td>{esc(last['source']) if last else '-'}</td><td>{last['row_count'] if last else '-'}</td>"
                f"<td>v{last['version'] if last else '-'}</td><td>{sched} {tog}</td><td>"
                f"<form class='inline' method='post' action='/admin/datasets/{esc(d['name'])}/run'><button>Run now</button></form> "
                f"<form class='inline' method='post' action='/admin/datasets/{esc(d['name'])}/rollback' "
                f"onsubmit=\"return confirm('Swap back to the previous version?')\"><button>Rollback</button></form></td></tr>")
        out.append(
            "<section><h2>Datasets &amp; schedules</h2><table><tr><th>Dataset</th><th>Last good update</th>"
            "<th>Source</th><th>Rows</th><th>Version</th><th>Schedule (UTC cron)</th><th></th></tr>"
            + ("".join(drows) or "<tr><td colspan=7>None yet.</td></tr>") + "</table>"
            "<form method='post' action='/admin/datasets' style='margin-top:10px'>"
            "New dataset: <input name='name' placeholder='name_lowercase' required> "
            "key columns <input name='key_columns' placeholder='id,date'> "
            "stale after (min) <input name='stale_after_minutes' type='number' value='1440' style='width:80px'> "
            "max row change % <input name='max_change_pct' type='number' value='50' style='width:70px'> "
            "schedule <input name='cron' placeholder='0 6 * * *'> <button>Save</button></form></section>")

        # recent versions
        vrows = store.rows(sa.select(v).order_by(v.c.id.desc()).limit(10))
        out.append(
            "<section><h2>Recent loads</h2><table><tr><th>When</th><th>Dataset</th><th>Result</th><th>Source</th>"
            "<th>Rows</th><th>Note</th></tr>"
            + "".join(f"<tr><td>{ago(x['loaded_at'], now)}</td><td>{esc(x['dataset'])} v{x['version']}</td>"
                      f"<td>{pill(x['status'])}</td><td>{esc(x['source'] or '')}</td><td>{x['row_count'] or ''}</td>"
                      f"<td>{esc(x['message'] or '')}</td></tr>" for x in vrows)
            + "</table></section>")
        return page("".join(out))

    # ----- actions -----
    @router.post("/agents", response_class=HTMLResponse)
    def add_agent(request: Request, name: str = Form(...), url: str = Form(...),
                  datasets: str = Form("*"), user: str = Depends(admin)):
        same_origin(request)
        try:
            _, secret, token = svc.register_agent(user, name.strip(), url.strip(), datasets.strip())
        except sa.exc.IntegrityError:
            return done("An agent with that name already exists.")
        return page(
            f"<section><h2>Agent “{esc(name)}” registered</h2><p>Copy these into the agent's config. "
            f"<b>They are shown only once.</b></p><p>secret: <code>{esc(secret)}</code></p>"
            f"<p>token: <code>{esc(token)}</code></p><p><a href='/admin'>Back</a></p></section>",
            refresh=False)

    @router.post("/agents/{agent_id}/rotate", response_class=HTMLResponse)
    def rotate(agent_id: int, request: Request, user: str = Depends(admin)):
        same_origin(request)
        secret, token = svc.rotate_token(user, agent_id)
        return page(
            f"<section><h2>New credentials</h2><p>secret: <code>{esc(secret)}</code></p>"
            f"<p>token: <code>{esc(token)}</code></p><p>Update the agent config; the old ones no longer work."
            f"</p><p><a href='/admin'>Back</a></p></section>", refresh=False)

    @router.post("/agents/{agent_id}/{action}")
    def agent_toggle(agent_id: int, action: str, request: Request, user: str = Depends(admin)):
        same_origin(request)
        if action not in ("enable", "disable"):
            raise HTTPException(404)
        svc.set_agent_enabled(user, agent_id, action == "enable")
        return done(f"Agent {action}d.")

    @router.post("/jobs/{job_id}/{action}")
    def job_action(job_id: int, action: str, request: Request, user: str = Depends(admin)):
        same_origin(request)
        if action == "cancel":
            return done("Job cancelled." if svc.cancel_job(user, job_id) else "Job not active.")
        if action == "retry":
            return done("Job queued." if svc.retry_job(user, job_id) else "Cannot retry that job.")
        raise HTTPException(404)

    @router.post("/datasets")
    def save_dataset(request: Request, name: str = Form(...), key_columns: str = Form(""),
                     stale_after_minutes: int = Form(1440), max_change_pct: str = Form(""),
                     cron: str = Form(""), user: str = Depends(admin)):
        same_origin(request)
        try:
            svc.upsert_dataset(
                user, name.strip(), key_columns=key_columns.strip(),
                stale_after_minutes=stale_after_minutes,
                max_change_pct=float(max_change_pct) if max_change_pct.strip() else None)
            if cron.strip():
                svc.set_schedule(user, name.strip(), cron.strip())
        except ValueError as exc:
            return done(f"Error: {exc}")
        return done("Dataset saved.")

    @router.post("/datasets/{name}/run")
    def run_now(name: str, request: Request, user: str = Depends(admin)):
        same_origin(request)
        try:
            job = svc.enqueue_job(name, "admin_run_now", user)
        except ValueError:
            return done("Unknown dataset.")
        return done(f"Job {job['id']} is {job['status']}.")

    @router.post("/datasets/{name}/rollback")
    def rollback(name: str, request: Request, user: str = Depends(admin)):
        same_origin(request)
        try:
            v = loader.rollback_dataset(store, name, user)
        except loader.ValidationError as exc:
            return done(str(exc))
        store.audit_log(user, "dataset.rollback", f"{name} -> v{v}")
        return done(f"{name} rolled back.")

    @router.post("/schedules/{name}/{action}")
    def schedule_toggle(name: str, action: str, request: Request, user: str = Depends(admin)):
        same_origin(request)
        if action not in ("pause", "resume"):
            raise HTTPException(404)
        svc.toggle_schedule(user, name, action == "resume")
        return done(f"Schedule {action}d.")

    return router
