"""Read-only SQL check for ad-hoc queries (used by both the service and the agent).

Allows a single statement that starts with SELECT / WITH / SHOW / DESCRIBE / EXPLAIN and
contains no write or admin keyword outside string literals and comments.
"""
import re

ALLOWED_START = {"select", "with", "show", "describe", "desc", "explain"}
FORBIDDEN = {
    "insert", "update", "delete", "drop", "create", "alter", "truncate", "merge", "grant",
    "revoke", "call", "set", "use", "load", "export", "import", "replace", "analyze", "msck",
    "refresh", "add", "reset", "kill", "lock", "unlock", "comment", "commit", "rollback",
    "execute", "prepare", "deallocate", "into",
}
_NOISE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|`[^`]*`|--[^\n]*|/\*.*?\*/", re.S)
_WORD = re.compile(r"[a-z_][a-z0-9_]*")


class UnsafeSQL(ValueError):
    """The statement is not a single read-only query."""


def check_read_only(sql):
    """Return the statement without a trailing ';', or raise UnsafeSQL."""
    if not sql or not sql.strip():
        raise UnsafeSQL("SQL is empty")
    if len(sql) > 20000:
        raise UnsafeSQL("SQL is too long (max 20,000 characters)")
    text = _NOISE.sub(" ", sql).strip().rstrip(";").strip()
    if any(q in text for q in ("'", '"', "`", "/*")):
        raise UnsafeSQL("unbalanced quote or comment")
    if ";" in text:
        raise UnsafeSQL("only one statement is allowed")
    words = _WORD.findall(text.lower())
    if not words or words[0] not in ALLOWED_START:
        raise UnsafeSQL("only SELECT, WITH, SHOW, DESCRIBE and EXPLAIN statements are allowed")
    bad = sorted(set(words) & FORBIDDEN)
    if bad:
        raise UnsafeSQL(f"read-only mode: keyword not allowed: {', '.join(bad)}")
    stripped = sql.strip()
    return stripped[:-1].rstrip() if stripped.endswith(";") else stripped
