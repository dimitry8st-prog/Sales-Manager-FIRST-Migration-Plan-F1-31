"""F1-07 migration: create the isolated ``f1_07_*`` schema and its single table.

Public entry point (frozen by the task)::

    def apply_migration(*, connect_factory, schema: str) -> None

The migration is transactional and repeatable (``CREATE ... IF NOT EXISTS``),
never drops data and never runs a dynamic ``ALTER`` on a foreign schema.  Both
``CREATE`` statements and the catalog verification below run in ONE transaction:
if the factory hands over a connection in autocommit mode the runner takes it out
of autocommit first, so a failure while creating the table - or a later
verification failure - rolls the newly created schema back as well instead of
leaving a committed empty schema behind.  After
the two ``CREATE`` statements it verifies, through the PostgreSQL catalog, the
expected columns/types/NOT NULL, the ``clock_timestamp()`` DEFAULT declared for
``stored_at``, the primary key and the two scoped UNIQUE constraints — and that
none of those constraints is ``DEFERRABLE``, because ``ON CONFLICT`` refuses
deferrable unique/PK constraints as arbiters (measured on PostgreSQL 16.15:
``ON CONFLICT does not support deferrable unique constraints/exclusion
constraints as arbiters``).  It also verifies the seven ``CHECK`` constraints of
the fixed DDL (task section 6, the DDL block and the sentence that names the
structure check): a pre-existing table without them, or with a weakened one, is
refused exactly like one with the wrong columns.  The comparison uses the
catalogue text, which is already the server's normalised form — spelling variants
the server folds together (``BETWEEN`` instead of ``>=``/``<=``, mixed case, an
explicit ``::text`` cast) are accepted, ``CHECK (true)`` is not.  An incompatible
structure (for example a pre-existing ``inbox_events`` table created by something
else — ``IF NOT EXISTS`` cannot fix it) raises
``InboxStoreError("INVALID_SCHEMA")``, the transaction is
rolled back and the existing table is left exactly as it was: this package never
ALTERs the structure it finds.

CLI (``python -m f1_07_inbox.migrate``) reads **only** ``F107_TEST_DSN`` and
``F107_SCHEMA``, refuses anything but the disposable local test target of the
task (``127.0.0.1`` + numeric port + dbname ``sm_f107_test`` + user
``f107_test``) and never prints the DSN or a password.  A missing DSN prints
``DB_NOT_RUN`` and exits non-zero.

The SQL file keeps the single ``{{schema}}`` placeholder of the task, which
means ``sql.Identifier`` — not user-controlled string substitution.  The runner
splits the file into exactly two statements and composes each one as
``sql.SQL(text) + sql.Identifier(schema)``, so the placeholder is never fed
through ``str.format``/``%`` and no incoming message is ever executed as SQL.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from f1_07_inbox.store import INVALID_SCHEMA, InboxStoreError, validate_schema

__all__ = [
    "LocalTargetError",
    "apply_migration",
    "local_connect_factory",
    "main",
    "parse_local_dsn",
]

MIGRATION_FILENAME = "001_inbox_events.sql"
MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
SCHEMA_PLACEHOLDER = "{{schema}}"
TABLE_NAME = "inbox_events"

LOCAL_HOST = "127.0.0.1"
LOCAL_DBNAME = "sm_f107_test"
LOCAL_USER = "f107_test"
PORT_RANGE = (1, 65535)
CONNECT_TIMEOUT_RANGE = (1, 300)
REQUIRED_DSN_KEYS = ("host", "port", "dbname", "user")
ALLOWED_DSN_KEYS = frozenset(
    {"host", "port", "dbname", "user", "password", "connect_timeout"}
)

EXPECTED_COLUMNS = (
    ("inbox_id", "uuid"),
    ("client_id", "text"),
    ("mailbox_ref", "text"),
    ("provider", "text"),
    ("message_id", "text"),
    ("event_key", "text"),
    ("payload_sha256", "text"),
    ("normalized_event", "jsonb"),
    ("stored_at", "timestamp with time zone"),
)
EXPECTED_PRIMARY_KEY = ("inbox_id",)
EXPECTED_UNIQUE_CONSTRAINTS = (
    ("client_id", "mailbox_ref", "provider", "message_id"),
    ("client_id", "mailbox_ref", "event_key"),
)
# The DDL fixed by task section 6 declares exactly this default.  PostgreSQL
# normalises the equivalent spellings (``clock_timestamp()``, ``(clock_timestamp())``,
# ``CLOCK_TIMESTAMP()``) to the same catalog text, while ``now()`` or a literal
# timestamp keep their own, so the check compares the normalised expression
# rather than the raw DDL text.
EXPECTED_STORED_AT_DEFAULT = "clock_timestamp()"
# Task section 6 fixes the DDL of the table, its CHECK constraints included, and
# asks apply_migration to verify the structure it finds through the catalogue.  A
# pre-existing table without these constraints - or with a weakened one - is an
# incompatible structure: it has to be refused instead of being accepted and
# failing later.  The strings below are the catalogue text of PostgreSQL 16.15
# (measured, see the module docstring), which is already the server's normalised
# form: spellings the server folds together (``BETWEEN`` instead of ``>=``/``<=``,
# mixed case, an explicit ``::text`` cast) still compare equal, while ``CHECK
# (true)`` or a missing constraint does not.
EXPECTED_CHECK_CONSTRAINTS = (
    "CHECK ((event_key ~ '^[0-9a-f]{64}$'::text))",
    "CHECK ((jsonb_typeof(normalized_event) = 'object'::text))",
    "CHECK (((octet_length(client_id) >= 1) AND (octet_length(client_id) <= 512)))",
    "CHECK (((octet_length(mailbox_ref) >= 1) AND (octet_length(mailbox_ref) <= 512)))",
    "CHECK (((octet_length(message_id) >= 1) AND (octet_length(message_id) <= 512)))",
    "CHECK (((octet_length(provider) >= 1) AND (octet_length(provider) <= 512)))",
    "CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text))",
)


def _normalise_sql_text(text: str) -> str:
    """Collapse whitespace only.

    The catalogue text is already the server's normalised form, so collapsing runs
    of whitespace is enough to compare it with the expected constants.  Case is
    deliberately left alone: the server lowercases identifiers itself, while the
    case inside a string literal (``= 'object'``) is meaningful.
    """
    return " ".join(text.split())


class LocalTargetError(ValueError):
    """Local test-target guard failure; ``str(error)`` is the code only."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


# --- guard for the disposable local target -----------------------------------


def _port_number(value: object) -> int:
    if isinstance(value, bool):
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    if isinstance(value, int):
        port = value
    elif isinstance(value, str) and value.isdigit():
        port = int(value)
    else:
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    if not PORT_RANGE[0] <= port <= PORT_RANGE[1]:
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    return port


def _timeout_number(value: object) -> int:
    if isinstance(value, bool):
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    if isinstance(value, int):
        timeout = value
    elif isinstance(value, str) and value.isdigit():
        timeout = int(value)
    else:
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    if not CONNECT_TIMEOUT_RANGE[0] <= timeout <= CONNECT_TIMEOUT_RANGE[1]:
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    return timeout


def parse_local_dsn(dsn: object) -> Dict[str, Any]:
    """Accept only the disposable local test DSN of the task.

    Rejected: unix socket, hostname/``localhost``, several hosts, ``hostaddr``,
    ``service``, URIs with unexpected parameters, non-numeric or out-of-range
    port, any dbname/user other than the frozen test ones.
    """
    if not isinstance(dsn, str) or dsn.strip() == "":
        raise LocalTargetError("INVALID_TEST_DSN")
    try:
        params = conninfo_to_dict(dsn)
    except (psycopg.ProgrammingError, psycopg.Error):
        raise LocalTargetError("INVALID_TEST_DSN") from None
    keys = set(params)
    if not keys <= ALLOWED_DSN_KEYS:
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    if not set(REQUIRED_DSN_KEYS) <= keys:
        raise LocalTargetError("INVALID_TEST_DSN")
    if params["host"] != LOCAL_HOST:
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    if params["dbname"] != LOCAL_DBNAME:
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    if params["user"] != LOCAL_USER:
        raise LocalTargetError("UNSAFE_LOCAL_TARGET")
    if "connect_timeout" in params:
        _timeout_number(params["connect_timeout"])
    params = dict(params)
    params["port"] = _port_number(params["port"])
    return params


def local_connect_factory(
    dsn: object, *, connect_timeout: Optional[int] = None
) -> Callable[[], psycopg.Connection]:
    """Factory returning a NEW connection to the verified local test database.

    Each connection is checked against ``current_database()``/``current_user()``
    before it is handed out: on a mismatch nothing is executed and the
    connection is closed.
    """
    params = parse_local_dsn(dsn)
    kwargs: Dict[str, Any] = {key: str(value) for key, value in params.items()}
    if connect_timeout is not None:
        kwargs["connect_timeout"] = str(_timeout_number(connect_timeout))

    def factory() -> psycopg.Connection:
        conn = psycopg.connect(**kwargs)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user")
                identity = cur.fetchone()
            conn.rollback()
        except BaseException:
            conn.close()
            raise
        if identity != (LOCAL_DBNAME, LOCAL_USER):
            conn.close()
            raise LocalTargetError("TARGET_MISMATCH")
        return conn

    return factory


# --- migration ---------------------------------------------------------------


def _migration_statements(schema: str) -> list:
    """Compose the two statements of the SQL file with ``sql.Identifier``."""
    raw = (MIGRATIONS_DIR / MIGRATION_FILENAME).read_text(encoding="utf-8")
    if raw.count(SCHEMA_PLACEHOLDER) != 2:
        raise InboxStoreError(INVALID_SCHEMA)
    head, rest = raw.split(SCHEMA_PLACEHOLDER, 1)
    between, tail = rest.split(SCHEMA_PLACEHOLDER, 1)
    between = between.lstrip(" \t\r\n;")
    if not head.upper().strip().startswith("CREATE SCHEMA"):
        raise InboxStoreError(INVALID_SCHEMA)
    if not between.upper().startswith("CREATE TABLE"):
        raise InboxStoreError(INVALID_SCHEMA)
    if head.strip() == "" or between == "" or tail.strip() == "":
        raise InboxStoreError(INVALID_SCHEMA)
    identifier = sql.Identifier(schema)
    return [
        sql.SQL(head) + identifier,
        sql.SQL(between) + identifier + sql.SQL(tail),
    ]


def _verify_migrated_structure(cur: Any, schema: str) -> None:
    cur.execute(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = %s",
        (schema, TABLE_NAME),
    )
    found = {
        row["column_name"]: (row["data_type"], row["is_nullable"])
        for row in cur.fetchall()
    }
    if set(found) != {name for name, _type in EXPECTED_COLUMNS}:
        raise InboxStoreError(INVALID_SCHEMA)
    for name, expected_type in EXPECTED_COLUMNS:
        data_type, is_nullable = found[name]
        if data_type != expected_type or is_nullable != "NO":
            raise InboxStoreError(INVALID_SCHEMA)

    # stored_at must carry the DEFAULT the task's DDL declares: the component
    # never writes stored_at itself, so a table whose stamp is not produced by
    # clock_timestamp() (or has no default at all) is an incompatible structure.
    try:
        cur.execute(
            "SELECT pg_get_expr(d.adbin, d.adrelid) AS default_expr"
            " FROM pg_attrdef d"
            " JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum"
            " WHERE d.adrelid = format('%%I.%%I', %s::text, %s::text)::regclass"
            " AND a.attname = 'stored_at'",
            (schema, TABLE_NAME),
        )
        defaults = cur.fetchall()
    except psycopg.errors.UndefinedTable:
        raise InboxStoreError(INVALID_SCHEMA) from None
    if len(defaults) != 1 or defaults[0]["default_expr"] != EXPECTED_STORED_AT_DEFAULT:
        raise InboxStoreError(INVALID_SCHEMA)

    try:
        cur.execute(
            "SELECT c.conname, c.contype, c.condeferrable, c.condeferred, a.attname"
            " FROM pg_constraint c"
            " JOIN pg_attribute a ON a.attrelid = c.conrelid"
            " AND a.attnum = ANY(c.conkey)"
            " WHERE c.conrelid = format('%%I.%%I', %s::text, %s::text)::regclass",
            (schema, TABLE_NAME),
        )
        rows = cur.fetchall()
    except psycopg.errors.UndefinedTable:
        raise InboxStoreError(INVALID_SCHEMA) from None

    # ON CONFLICT DO NOTHING, which append relies on, cannot use a deferrable
    # unique or primary-key constraint as an arbiter ("ON CONFLICT does not
    # support deferrable unique constraints/exclusion constraints as arbiters"),
    # so such a table is refused instead of being accepted and failing later.
    if any(row["condeferrable"] or row["condeferred"] for row in rows):
        raise InboxStoreError(INVALID_SCHEMA)

    groups: Dict[tuple, set] = {}
    for row in rows:
        groups.setdefault((row["contype"], row["conname"]), set()).add(row["attname"])
    primary = [cols for (kind, _name), cols in groups.items() if kind == "p"]
    uniques = [cols for (kind, _name), cols in groups.items() if kind == "u"]
    if len(primary) != 1 or primary[0] != set(EXPECTED_PRIMARY_KEY):
        raise InboxStoreError(INVALID_SCHEMA)
    if len(uniques) != 2:
        raise InboxStoreError(INVALID_SCHEMA)
    wanted = sorted(tuple(sorted(cols)) for cols in EXPECTED_UNIQUE_CONSTRAINTS)
    actual = sorted(tuple(sorted(cols)) for cols in uniques)
    if actual != wanted:
        raise InboxStoreError(INVALID_SCHEMA)

    # Task section 6 fixes the DDL of the table, its CHECK constraints included, and
    # asks apply_migration to verify the structure it finds through the catalogue:
    # a pre-existing table whose CHECK constraints are missing or weakened is an
    # incompatible structure and is refused here, not patched by an automatic ALTER.
    # The expected set has to match exactly, because an *extra* constraint would
    # reject rows this component is entitled to store, and a missing one would let
    # the database accept values the DDL forbids.
    try:
        cur.execute(
            "SELECT pg_get_constraintdef(c.oid) AS constraint_def"
            " FROM pg_constraint c"
            " WHERE c.conrelid = format('%%I.%%I', %s::text, %s::text)::regclass"
            " AND c.contype = 'c'",
            (schema, TABLE_NAME),
        )
        check_rows = cur.fetchall()
    except psycopg.errors.UndefinedTable:
        raise InboxStoreError(INVALID_SCHEMA) from None
    found_checks = sorted(
        _normalise_sql_text(row["constraint_def"]) for row in check_rows
    )
    if found_checks != sorted(EXPECTED_CHECK_CONSTRAINTS):
        raise InboxStoreError(INVALID_SCHEMA)


def apply_migration(*, connect_factory: Callable[[], Any], schema: str) -> None:
    """Create schema + ``inbox_events`` in ONE transaction, then verify catalog."""
    validate_schema(schema)
    statements = _migration_statements(schema)
    conn = connect_factory()
    try:
        # Task section 6: the migration is transactional, so ``CREATE SCHEMA`` and
        # ``CREATE TABLE`` must belong to ONE transaction - otherwise the schema is
        # already committed when the table statement (or the catalog verification
        # below) fails and the rollback leaves it behind.  A factory that hands back
        # an autocommit connection would commit each statement on its own and make
        # ``commit()`` a no-op, so the precondition is enforced here instead of only
        # documented, exactly as ``append`` does for its own transaction.
        # psycopg allows the switch while the connection is idle.
        if conn.autocommit:
            conn.autocommit = False
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement, prepare=False)
        with conn.cursor(row_factory=dict_row) as cur:
            _verify_migrated_structure(cur, schema)
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except Exception:  # pragma: no cover - defensive
            pass
        raise
    finally:
        conn.close()


def main(argv: Optional[list] = None) -> int:
    """CLI: env F107_TEST_DSN + F107_SCHEMA only; never prints the DSN."""
    dsn = os.environ.get("F107_TEST_DSN")
    schema = os.environ.get("F107_SCHEMA")
    if not dsn or not schema:
        print("DB_NOT_RUN", flush=True)
        return 2
    try:
        validate_schema(schema)
        factory = local_connect_factory(dsn)
        apply_migration(connect_factory=factory, schema=schema)
    except LocalTargetError as exc:
        print("MIGRATION_REJECTED %s" % exc.code, flush=True)
        return 3
    except InboxStoreError as exc:
        print("MIGRATION_REJECTED %s" % exc.code, flush=True)
        return 3
    print("MIGRATION_APPLIED schema=%s" % schema, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
