"""Management dashboard (server-rendered HTML, auto-refreshing every 10s)."""
import csv
import html
import io
import json
import secrets
from urllib.parse import urlparse

import sqlalchemy as sa
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
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
nav{margin-top:6px}nav a{color:#9fc2ff;margin-right:14px;text-decoration:none}
"""

SQL_HEAD = """
<link rel="stylesheet" href="/static/cm/codemirror.min.css">
<link rel="stylesheet" href="/static/cm/addon/show-hint.min.css">
<style>
.sqlwrap{display:grid;grid-template-columns:1fr 260px;gap:18px}@media(max-width:900px){.sqlwrap{grid-template-columns:1fr}}
.CodeMirror{height:240px;border:1px solid #c5cbd5;border-radius:6px;font-size:14px}
#grid{overflow:auto;max-height:480px;border:1px solid #dde1e8;border-radius:6px}
#grid table{min-width:100%}#grid th{position:sticky;top:0;background:#f0f2f6}
#hist{list-style:none;padding:0;margin:0}#hist li{padding:6px 0;border-bottom:1px solid #eceff3}
#hist a{color:#1c2330;text-decoration:none;word-break:break-word}
.bar{display:flex;gap:8px;align-items:center;margin:10px 0;flex-wrap:wrap}
button.primary{background:#2563eb;color:#fff;border-color:#2563eb;padding:5px 16px}
#st{color:#5b6677}.err{color:#b42318;white-space:pre-wrap}
</style>
<script src="/static/cm/codemirror.min.js"></script>
<script src="/static/cm/mode/sql.min.js"></script>
<script src="/static/cm/addon/show-hint.min.js"></script>
<script src="/static/cm/addon/sql-hint.min.js"></script>
"""

SQL_PAGE = """
<div class="sqlwrap"><div>
<section><h2>SQL test <small style="font-weight:normal;color:#5b6677">
read-only (SELECT / WITH / SHOW / DESCRIBE / EXPLAIN), first 10,000 rows, runs on a company PC</small></h2>
<textarea id="sql">select 1</textarea>
<div class="bar">
 <select id="driver"><option value="presto">Presto</option><option value="hive">Hive</option></select>
 <button class="primary" id="run">Run (Ctrl+Enter)</button>
 <button id="cancel" disabled>Cancel</button>
 <a id="csv" href="#" style="display:none">Download CSV</a>
 <span id="st"></span>
</div>
<div id="msg" class="err"></div>
<div id="grid"></div></section></div>
<aside><section><h2>Recent</h2><ul id="hist">{{HISTORY}}</ul></section></aside></div>
<script>
const $=id=>document.getElementById(id), ta=$('sql');
const ed=window.CodeMirror?CodeMirror.fromTextArea(ta,{mode:'text/x-sql',lineNumbers:true,
 indentWithTabs:false,extraKeys:{'Ctrl-Enter':run,'Cmd-Enter':run,'Ctrl-Space':'autocomplete'},
 hintOptions:{completeSingle:false}}):null;
if(ed)ed.on('inputRead',(c,ch)=>{if(/[A-Za-z_.]/.test(ch.text[0]||''))c.showHint({completeSingle:false})});
const getSql=()=>ed?ed.getValue():ta.value, setSql=v=>ed?ed.setValue(v):(ta.value=v);
let cur=null,runningId=0;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
function show(j){
 $('msg').textContent='';const g=$('grid');g.textContent='';$('csv').style.display='none';
 if(j.status!=='success'){$('msg').textContent=j.status==='cancelled'?'Cancelled.':(j.error||'');return}
 const MAX=1000,t=document.createElement('table'),h=t.insertRow();
 j.columns.forEach(c=>{const th=document.createElement('th');th.textContent=c;h.appendChild(th)});
 j.rows.slice(0,MAX).forEach(r=>{const tr=t.insertRow();r.forEach(v=>{tr.insertCell().textContent=v===null?'NULL':String(v)})});
 g.appendChild(t);
 const more=j.rows.length>MAX?` (showing first ${MAX}; download CSV for all)`:'';
 $('st').textContent=`${j.row_count} rows${j.truncated?' (capped at 10,000)':''} in ${j.elapsed}s${more}`;
 $('csv').href=`/admin/sql/${j.id}/csv`;$('csv').style.display='inline';
}
async function run(){
 if($('run').disabled)return;
 $('run').disabled=true;$('cancel').disabled=false;$('msg').textContent='';$('st').textContent='Sending…';
 $('grid').textContent='';$('csv').style.display='none';
 try{
  const r=await fetch('/admin/sql/run',{method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify({driver:$('driver').value,sql:getSql()})});
  const j=await r.json();
  if(!r.ok){$('msg').textContent=j.detail||'Request failed';$('st').textContent='';return}
  cur=j.id;
  for(;;){
   const s=await (await fetch('/admin/sql/'+cur)).json();
   if(s.status==='pending'||s.status==='running'){$('st').textContent=`Running… ${s.elapsed}s`;await sleep(1000);continue}
   $('st').textContent=s.status==='success'?'':s.status;show(s);break;
  }
 }catch(e){$('msg').textContent=String(e)}
 finally{$('run').disabled=false;$('cancel').disabled=true;cur=null}
}
$('run').onclick=run;
$('cancel').onclick=()=>{if(cur)fetch('/admin/sql/'+cur+'/cancel',{method:'POST'})};
$('hist').onclick=e=>{const a=e.target.closest('a');if(!a)return;e.preventDefault();
 setSql(a.dataset.sql);$('driver').value=a.dataset.driver};
</script>
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

    def page(body, refresh=True, head=""):
        meta = '<meta http-equiv="refresh" content="10">' if refresh else ""
        return HTMLResponse(
            f'<!doctype html><html><head><meta charset="utf-8">{meta}'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">'
            f"<title>Relay admin</title><style>{CSS}</style>{head}</head><body>"
            f'<header><h1>data-gateway · management</h1>'
            f'<nav><a href="/admin">Overview</a> <a href="/admin/sql">SQL test</a></nav></header>'
            f"<main>{body}</main></body></html>"
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

    # ----- SQL test page (read-only; runs on an agent that allows ad-hoc SQL) -----
    @router.get("/sql", response_class=HTMLResponse)
    def sql_page(user: str = Depends(admin)):
        now = utcnow()
        items = []
        for x in svc.recent_adhoc(15):
            first = " ".join(x["sql"].split())[:90]
            items.append(
                f"<li><a href='#' data-sql=\"{esc(x['sql'], quote=True)}\" data-driver='{esc(x['driver'])}'>"
                f"{esc(first)}</a><br><small>{pill(x['status'])} {esc(x['driver'])} · "
                f"{ago(x['created_at'], now)}"
                f"{' · ' + str(x['row_count']) + ' rows' if x['row_count'] is not None else ''}"
                f"</small></li>")
        body = SQL_PAGE.replace("{{HISTORY}}", "".join(items) or "<li><small>No queries yet.</small></li>")
        return page(body, refresh=False, head=SQL_HEAD)

    @router.post("/sql/run")
    async def sql_run(request: Request, user: str = Depends(admin)):
        same_origin(request)
        try:
            data = await request.json()
            qid = svc.submit_adhoc(user, str(data.get("driver", "")), str(data.get("sql", "")))
        except ValueError as exc:  # also covers bad JSON and UnsafeSQL
            raise HTTPException(400, str(exc))
        return {"id": qid}

    def _query_json(qid):
        row = svc.adhoc_status(qid)
        if not row:
            raise HTTPException(404, "unknown query")
        out = {"id": row["id"], "status": row["status"], "error": row["error"],
               "driver": row["driver"], "row_count": row["row_count"],
               "truncated": bool(row["truncated"]),
               "elapsed": int(((row["finished_at"] or utcnow()) - (row["started_at"] or row["created_at"]))
                              .total_seconds()),
               "columns": [], "rows": []}
        if row["status"] == "success" and row["result_json"]:
            res = json.loads(row["result_json"])
            out["columns"], out["rows"] = res["columns"], res["rows"]
        return out

    @router.get("/sql/{qid}")
    def sql_status(qid: int, user: str = Depends(admin)):
        return _query_json(qid)

    @router.get("/sql/{qid}/csv")
    def sql_csv(qid: int, user: str = Depends(admin)):
        res = _query_json(qid)
        if res["status"] != "success":
            raise HTTPException(404, "no result")
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(res["columns"])
        for r in res["rows"]:  # a leading = + - @ would run as a formula in Excel
            w.writerow(["'" + c if isinstance(c, str) and c[:1] in "=+-@" else c for c in r])
        return Response(buf.getvalue(), media_type="text/csv", headers={
            "Content-Disposition": f"attachment; filename=query-{qid}.csv"})

    @router.post("/sql/{qid}/cancel")
    def sql_cancel(qid: int, request: Request, user: str = Depends(admin)):
        same_origin(request)
        return {"cancelled": svc.cancel_adhoc(user, qid)}

    @router.post("/schedules/{name}/{action}")
    def schedule_toggle(name: str, action: str, request: Request, user: str = Depends(admin)):
        same_origin(request)
        if action not in ("pause", "resume"):
            raise HTTPException(404)
        svc.toggle_schedule(user, name, action == "resume")
        return done(f"Schedule {action}d.")

    return router
