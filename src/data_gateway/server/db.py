"""Database layer (SQLAlchemy Core; Postgres in production, SQLite in tests)."""
import sqlalchemy as sa

from ..common import utcnow


class Store:
    """Holds the engine and table definitions.

    ``schema``      - schema for the relay's own tables (Postgres: ``relay``).
    ``data_schema`` - schema where dashboard-facing dataset tables are written.
    """

    def __init__(self, url, schema=None, data_schema=None):
        self.engine = sa.create_engine(url, future=True, pool_pre_ping=True)
        if schema is None and self.engine.dialect.name == "postgresql":
            schema = "relay"
        self.schema = schema
        self.data_schema = data_schema
        md = self.md = sa.MetaData(schema=schema)
        C, T = sa.Column, sa.Table
        self.agents = T(
            "agents", md,
            C("id", sa.Integer, primary_key=True, autoincrement=True),
            C("name", sa.String(80), unique=True, nullable=False),
            C("url", sa.String(300), nullable=False),
            C("secret", sa.String(200), nullable=False),
            C("token_hash", sa.String(64), nullable=False, index=True),
            C("enabled", sa.Boolean, nullable=False, default=True),
            C("status", sa.String(20), nullable=False, default="unknown"),
            C("busy", sa.Boolean, nullable=False, default=False),
            C("version", sa.String(40)),
            C("last_seen", sa.DateTime),
            C("datasets", sa.Text, nullable=False, default="*"),
            C("created_at", sa.DateTime, nullable=False, default=utcnow),
        )
        self.datasets = T(
            "datasets", md,
            C("name", sa.String(40), primary_key=True),
            C("description", sa.Text, default=""),
            C("key_columns", sa.Text, default=""),
            C("min_rows", sa.Integer, nullable=False, default=1),
            C("max_change_pct", sa.Float),
            C("stale_after_minutes", sa.Integer, nullable=False, default=1440),
            C("max_duration_minutes", sa.Integer, nullable=False, default=30),
            C("allow_schema_change", sa.Boolean, nullable=False, default=False),
        )
        self.schedules = T(
            "schedules", md,
            C("dataset", sa.String(40), primary_key=True),
            C("cron", sa.String(80), nullable=False),
            C("enabled", sa.Boolean, nullable=False, default=True),
            C("last_run", sa.DateTime),
        )
        self.jobs = T(
            "jobs", md,
            C("id", sa.Integer, primary_key=True, autoincrement=True),
            C("dataset", sa.String(40), nullable=False, index=True),
            C("trigger", sa.String(40), nullable=False),
            C("requested_by", sa.String(80)),
            C("status", sa.String(20), nullable=False, index=True),
            C("agent_id", sa.Integer),
            C("attempts", sa.Integer, nullable=False, default=0),
            C("created_at", sa.DateTime, nullable=False, default=utcnow),
            C("started_at", sa.DateTime),
            C("finished_at", sa.DateTime),
            C("error", sa.Text),
            C("rows", sa.Integer),
        )
        self.versions = T(
            "dataset_versions", md,
            C("id", sa.Integer, primary_key=True, autoincrement=True),
            C("dataset", sa.String(40), nullable=False, index=True),
            C("version", sa.Integer, nullable=False),
            C("loaded_at", sa.DateTime, nullable=False, default=utcnow),
            C("source", sa.String(120)),
            C("job_id", sa.Integer),
            C("row_count", sa.Integer),
            C("sha256", sa.String(64)),
            C("status", sa.String(20), nullable=False),  # live | rejected | rollback
            C("message", sa.Text),
        )
        self.audit = T(
            "audit_log", md,
            C("id", sa.Integer, primary_key=True, autoincrement=True),
            C("ts", sa.DateTime, nullable=False, default=utcnow),
            C("actor", sa.String(80)),
            C("action", sa.String(60)),
            C("detail", sa.Text),
        )

        self.adhoc = T(
            "adhoc_queries", md,
            C("id", sa.Integer, primary_key=True, autoincrement=True),
            C("actor", sa.String(80)),
            C("driver", sa.String(20), nullable=False),
            C("sql", sa.Text, nullable=False),
            C("status", sa.String(20), nullable=False),  # pending|running|success|failed|cancelled
            C("agent_id", sa.Integer),
            C("created_at", sa.DateTime, nullable=False, default=utcnow),
            C("started_at", sa.DateTime),
            C("finished_at", sa.DateTime),
            C("error", sa.Text),
            C("row_count", sa.Integer),
            C("truncated", sa.Boolean),
            C("result_json", sa.Text),
        )

    def create_all(self):
        with self.engine.begin() as conn:
            if self.schema:
                conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"'))
            if self.data_schema:
                conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{self.data_schema}"'))
        self.md.create_all(self.engine)

    # ----- small helpers used everywhere -----
    def rows(self, stmt):
        with self.engine.begin() as conn:
            return [dict(r) for r in conn.execute(stmt).mappings().all()]

    def row(self, stmt):
        with self.engine.begin() as conn:
            r = conn.execute(stmt).mappings().first()
            return dict(r) if r else None

    def execute(self, stmt):
        with self.engine.begin() as conn:
            return conn.execute(stmt)

    def insert(self, table, **values):
        with self.engine.begin() as conn:
            return conn.execute(sa.insert(table).values(**values)).inserted_primary_key[0]

    def audit_log(self, actor, action, detail=""):
        self.insert(self.audit, actor=actor, action=action, detail=str(detail)[:1000], ts=utcnow())
