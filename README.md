# data-gateway

Moves data from a company PC (which can reach Presto / Hive) to the server that hosts the dashboards (which cannot).

The PC **agent is passive**: the server pings `GET /alive`, then sends `POST /task`. The agent runs the query with its Presto or Hive connector and uploads the result. The server validates it, loads it into Postgres through a staging table and swaps it in atomically, so bad data never replaces good data. Dashboards keep showing the last good data when no PC is available.

```
Dashboard refresh button / daily schedule -> job queue -> dispatcher
   dispatcher: ping PC (/alive) -> send task (/task) -> PC runs query -> PC uploads -> validate -> staging -> swap
```

## Parts
* `data_gateway.agent` - runs on the company PC (`/alive`, `/task`, `/task/{id}`, `/task/{id}/cancel`). Requests are HMAC-signed, and it only runs datasets from its local config.
* `data_gateway.connectors` - `PrestoConnector` and `HiveConnector` (adapted from `didi99`). Hosts come from the agent's local config; credentials from `HADOOP_USER_NAME` / `HADOOP_USER_PASSWORD`.
* `data_gateway.server` - FastAPI service: upload endpoint, job queue, dispatcher, scheduler (cron, UTC), health checks, safe loader with versions and rollback.
* **Management dashboard** at `/admin`: plain-language state summary, registered computers (online/busy/offline), jobs (retry/cancel), datasets (stale warning, run now, rollback), schedules (pause/resume).

* **SQL test page** at `/admin/sql`: an open-source SQL editor (CodeMirror 5, MIT, bundled locally so no internet is needed) with highlighting and autocomplete. Pick Presto or Hive, press Ctrl+Enter, and the service sends the query to an online agent, which runs it and returns the rows (table view, CSV download, recent history).
  * **Read-only and opt-in.** The agent runs ad-hoc SQL only if its `agent.yaml` has `allow_adhoc_sql: true`. Only a single `SELECT` / `WITH` / `SHOW` / `DESCRIBE` / `EXPLAIN` statement is accepted (checked on the server and again on the agent), results are capped at `adhoc_max_rows` (default 10,000), nothing is written to the dataset tables, and every run is in the audit log.

## Run
No Docker needed: plain Python (3.9+) in a virtualenv.

**Server (next to the dashboards' Postgres)**
```
python -m venv .venv
.venv/bin/pip install ".[server]"                 # Windows: .venv\Scripts\pip install ".[server]"
cp deploy/relay.env.example relay.env             # fill RELAY_DATABASE_URL and RELAY_ADMIN_TOKEN
```
* *Linux, always on:* edit paths in `deploy/relay.service`, copy it to `/etc/systemd/system/data-gateway.service`, put the env file at `/etc/data-gateway/relay.env`, then `sudo systemctl enable --now data-gateway`.
* *Windows Server, always on:* `.\deploy\run.ps1 -Role server -EnvFile relay.env` (restarts on exit); run it at boot with Task Scheduler or NSSM.
* *Quick test:* `set -a; . ./relay.env; set +a; .venv/bin/python -m data_gateway.server`

Open `http://<server>:8080/admin` (user `admin`, password = admin token), create a dataset, then register the PC to get its secret and token (shown once). Keep `RELAY_HOST=127.0.0.1` and put a TLS reverse proxy (nginx/Caddy/IIS) in front, or accept plain HTTP only on a trusted network.

**Agent (company PC, Windows)**
```
python -m venv .venv
.venv\Scripts\pip install ".[agent]"
copy deploy\agent.example.yaml agent.yaml         # fill name, relay_url, secret, token, connections, datasets
# agent.env: HADOOP_USER_NAME=...  and  HADOOP_USER_PASSWORD=...   (one KEY=VALUE per line)
.\deploy\install_agent_task.ps1 -InstallDir C:\data-gateway    # starts at logon, hidden, auto-restart
```
Manual run instead: `.venv\Scripts\python -m data_gateway.agent agent.yaml --port 8765`. Allow only the server's IP through the PC firewall for that port.

**Dashboard refresh button:** `POST /api/refresh/<dataset>` with `Authorization: Bearer <RELAY_DASHBOARD_TOKEN>`, then poll `GET /api/jobs/<id>`.

## Test
```
pip install -e ".[test]"
python -m unittest discover -s tests
```
