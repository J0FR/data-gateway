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

## Run
**Server (Docker, next to the dashboards' Postgres)**
```
cp deploy/.env.example deploy/.env      # fill RELAY_DATABASE_URL and RELAY_ADMIN_TOKEN
docker compose -f deploy/docker-compose.yml up -d
```
Open `http://<server>:8080/admin` (user `admin`, password = admin token), create a dataset, then register the PC to get its secret and token (shown once).

**Agent (company PC)**
```
pip install ".[agent]"
copy deploy\agent.example.yaml agent.yaml    # fill name, relay_url, secret, token, connections, datasets
set HADOOP_USER_NAME=...  &  set HADOOP_USER_PASSWORD=...
python -m data_gateway.agent agent.yaml --port 8765
```
Allow only the server's IP through the PC firewall, and put a TLS reverse proxy in front of the server.

**Dashboard refresh button:** `POST /api/refresh/<dataset>` with `Authorization: Bearer <RELAY_DASHBOARD_TOKEN>`, then poll `GET /api/jobs/<id>`.

## Test
```
pip install -e ".[test]"
python -m unittest discover -s tests
```
