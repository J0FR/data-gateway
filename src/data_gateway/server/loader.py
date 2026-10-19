"""Validation and the safe load of uploaded data into Postgres.

An upload is written to a staging table, validated, and only then swapped into the
live table inside ONE transaction. Any failure leaves the live table untouched.
"""
import io
import re
import zlib

import pandas as pd
import sqlalchemy as sa

from ..common import sha256_hex, utcnow

NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class ValidationError(Exception):
    """The uploaded data was rejected; the live data is unchanged."""


def decode_body(raw, gzipped, max_bytes):
    """Return the (decompressed) JSON bytes, refusing anything over ``max_bytes``."""
    if len(raw) > max_bytes:
        raise ValidationError("upload too large")
    if not gzipped:
        return raw
    d = zlib.decompressobj(wbits=31)
    try:
        out = d.decompress(raw, max_bytes + 1)
    except zlib.error:
        raise ValidationError("body is not valid gzip")
    if len(out) > max_bytes or d.unconsumed_tail:
        raise ValidationError("upload too large after decompression")
    return out


def parse_upload(payload):
    try:
        df = pd.read_json(io.StringIO(payload.decode("utf-8")), orient="split")
    except Exception as exc:
        raise ValidationError(f"cannot parse data: {exc}")
    return df


def _satype(series):
    k = series.dtype.kind
    if k in "iu":
        return sa.BigInteger()
    if k == "f":
        return sa.Float()
    if k == "b":
        return sa.Boolean()
    if k == "M":
        return sa.DateTime()
    return sa.Text()


def _split_keys(text):
    return [c.strip() for c in (text or "").split(",") if c.strip()]


def validate(df, cfg, live_columns=None, live_rows=None):
    """Raise ValidationError unless ``df`` is safe to publish."""
    if df.empty:
        raise ValidationError("upload has no rows")
    if len(df) < (cfg.get("min_rows") or 1):
        raise ValidationError(f"only {len(df)} rows, minimum is {cfg['min_rows']}")
    cols = list(df.columns)
    if len(set(cols)) != len(cols):
        raise ValidationError("duplicate column names")
    bad = [c for c in cols if not isinstance(c, str) or not COLUMN_RE.match(c)]
    if bad:
        raise ValidationError(f"invalid column names: {bad[:5]}")
    keys = _split_keys(cfg.get("key_columns"))
    missing = [k for k in keys if k not in cols]
    if missing:
        raise ValidationError(f"missing key columns: {missing}")
    for k in keys:
        if df[k].isna().any():
            raise ValidationError(f"key column {k!r} contains nulls")
    if keys and df.duplicated(subset=keys).any():
        raise ValidationError(f"duplicate rows for key {keys}")
    if live_columns is not None and not cfg.get("allow_schema_change"):
        if sorted(live_columns) != sorted(cols):
            raise ValidationError(
                f"columns changed (expected {sorted(live_columns)}, got {sorted(cols)})"
            )
    pct = cfg.get("max_change_pct")
    if pct is not None and live_rows:
        change = abs(len(df) - live_rows) * 100.0 / live_rows
        if change > pct:
            raise ValidationError(
                f"row count changed {change:.0f}% ({live_rows} -> {len(df)}), limit is {pct:.0f}%"
            )


def _qual(store, name):
    p = store.engine.dialect.identifier_preparer
    return f"{p.quote_schema(store.data_schema)}.{p.quote(name)}" if store.data_schema else p.quote(name)


def _records(df):
    clean = df.astype(object).where(pd.notna(df), None)
    return clean.to_dict(orient="records")


def _next_version(store, dataset):
    r = store.row(
        sa.select(sa.func.max(store.versions.c.version).label("v")).where(
            store.versions.c.dataset == dataset
        )
    )
    return (r["v"] or 0) + 1


def _record(store, conn, dataset, version, source, job_id, rows, digest, status, message):
    conn.execute(
        sa.insert(store.versions).values(
            dataset=dataset, version=version, loaded_at=utcnow(), source=source, job_id=job_id,
            row_count=rows, sha256=digest, status=status, message=message,
        )
    )


def record_rejection(store, dataset, source, job_id, digest, message):
    with store.engine.begin() as conn:
        _record(store, conn, dataset, _next_version(store, dataset), source, job_id, None,
                digest, "rejected", message[:500])


def load_dataset(store, cfg, df, source, job_id=None, digest=None):
    """Validate ``df`` and publish it. Returns the new version number.

    Raises ValidationError (live data untouched) if anything is wrong.
    """
    name = cfg["name"]
    if not NAME_RE.match(name):
        raise ValidationError("invalid dataset name")
    live, prev, stg = name, f"{name}_prev", f"_stg_{name}"
    insp = sa.inspect(store.engine)
    exists = insp.has_table(live, schema=store.data_schema)
    live_cols = [c["name"] for c in insp.get_columns(live, schema=store.data_schema)] if exists else None
    live_rows = None
    if exists:
        with store.engine.begin() as conn:
            live_rows = conn.execute(sa.text(f"SELECT COUNT(*) FROM {_qual(store, live)}")).scalar()
    validate(df, cfg, live_cols, live_rows)

    version = _next_version(store, name)
    table = sa.Table(stg, sa.MetaData(), *[sa.Column(c, _satype(df[c])) for c in df.columns],
                     schema=store.data_schema)
    q = lambda n: _qual(store, n)  # noqa: E731
    unq = store.engine.dialect.identifier_preparer.quote

    # 1. write staging (not visible to dashboards)
    with store.engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {q(stg)}"))
        table.create(conn)
        recs = _records(df)
        for i in range(0, len(recs), 5000):
            conn.execute(table.insert(), recs[i:i + 5000])

    # 2. swap into place atomically
    try:
        with store.engine.begin() as conn:
            if exists:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {q(prev)}"))
                conn.execute(sa.text(f"ALTER TABLE {q(live)} RENAME TO {unq(prev)}"))
            conn.execute(sa.text(f"ALTER TABLE {q(stg)} RENAME TO {unq(live)}"))
            _record(store, conn, name, version, source, job_id, len(df),
                    digest or "", "live", "")
    except Exception:
        with store.engine.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {q(stg)}"))
        raise
    return version


def rollback_dataset(store, name, actor):
    """Swap the live table with the previous one. Raises ValidationError if none."""
    insp = sa.inspect(store.engine)
    live, prev, tmp = name, f"{name}_prev", f"_swap_{name}"
    if not insp.has_table(prev, schema=store.data_schema):
        raise ValidationError("no previous version to roll back to")
    q = lambda n: _qual(store, n)  # noqa: E731
    unq = store.engine.dialect.identifier_preparer.quote
    version = _next_version(store, name)
    with store.engine.begin() as conn:
        conn.execute(sa.text(f"ALTER TABLE {q(live)} RENAME TO {unq(tmp)}"))
        conn.execute(sa.text(f"ALTER TABLE {q(prev)} RENAME TO {unq(live)}"))
        conn.execute(sa.text(f"ALTER TABLE {q(tmp)} RENAME TO {unq(prev)}"))
        rows = conn.execute(sa.text(f"SELECT COUNT(*) FROM {q(live)}")).scalar()
        _record(store, conn, name, version, f"rollback:{actor}", None, rows, "", "rollback",
                "swapped live and previous tables")
    return version
