"""F1-16 R3 - atomic task lease with a monotonically growing generation.

Scope (component only)
----------------------
``TaskLeases`` keeps one row per ``task_key`` in ``<schema>.task_leases`` (see
``migrations/001_task_leases.sql``).  It does not touch ``task_store`` or the
worker; an integrator must call ``assert_owned`` and ``finish_owned_in_tx``
inside the very transaction that performs the protected write.

Public contract (``FIRST_WAVE3_16_V3``)
---------------------------------------
::

    class TaskLeaseError(ValueError):
        code: str                 # str(error) == code, nothing else in the text

    TaskLeases(*, connect_factory, schema)                    # INVALID_SCHEMA
    acquire(*, task_key, owner_token, ttl_seconds) -> dict    # ACQUIRED | BUSY
    renew(*, task_key, owner_token, generation, ttl_seconds)  # RENEWED | STALE
    finish(*, task_key, owner_token, generation)              # FINISHED | STALE
    assert_owned(conn, *, task_key, owner_token, generation)  # None | STALE_OWNER
    finish_owned_in_tx(conn, *, task_key, owner_token, generation)  # None | STALE_OWNER

Clock
-----
The only time authority is PostgreSQL ``clock_timestamp()``.  The decision clock
is read exactly once per decision and only after the deciding transaction owns
the row: after ``SELECT ... FOR UPDATE`` for an existing row, or after its own
``INSERT`` of a first row succeeded (a conflicting concurrent insert has been
waited out by then).  A transaction that waited for a lock therefore never uses a
time taken before the wait.  A lease is active only while
``expires_at > decision_clock``; equality is already expired.  No caller supplied
time exists in any signature.

Semantics
---------
* ``BUSY`` reports ``generation`` and ``expires_at`` of the current lease and
  ``owner_token: null``.  A holder that calls ``acquire`` again while its own
  lease is active also gets ``BUSY`` and nothing is written; extension is only
  ``renew``.
* ``renew`` sets ``expires_at = decision_clock + ttl``; the previous expiry is not
  a base, so the lease may be shortened.  The generation never changes.
* ``finish`` keeps the row and sets ``expires_at`` to the decision clock.  The
  tuple is then stale for ``renew``/``finish``/``assert_owned`` and the next
  ``acquire`` takes the lease over with ``generation + 1``.
* ``finish_owned_in_tx`` does the same inside the caller's transaction, without
  committing, rolling back or closing.  The public ``finish`` owns its own
  transaction and must never be used as a separate step before the protected
  task transition.
* A first ``acquire`` of two connections is serialized by
  ``INSERT ... ON CONFLICT DO NOTHING``; the loser repeats lock + clock + decision.

Transactions
------------
``acquire``/``renew``/``finish`` use one fresh connection and one READ COMMITTED
transaction each and report a result only after ``commit()`` returned.  Any
failure, including a failing or ambiguous commit, is ``DB_ERROR``.
``assert_owned`` and ``finish_owned_in_tx`` run in the caller's transaction and
never commit, roll back or close anything; the row lock stays until the caller
ends its transaction.  Both require an ALREADY ACTIVE caller-owned transaction
(``transaction_status == INTRANS``) before any SQL; ``autocommit=False`` alone is
not enough.  IDLE, ACTIVE, INERROR, UNKNOWN or an unreadable status, and an
autocommit connection outside an explicit transaction, are ``DB_ERROR`` and are
rejected before a cursor is created::

    with conn.transaction():
        leases.assert_owned(conn, task_key=..., owner_token=..., generation=...)
        ...  # protected task mutation
        leases.finish_owned_in_tx(conn, task_key=..., owner_token=..., generation=...)
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

__all__ = ["TaskLeaseError", "TaskLeases"]

INVALID_SCHEMA = "INVALID_SCHEMA"
INVALID_INPUT = "INVALID_INPUT"
DB_ERROR = "DB_ERROR"
STALE_OWNER = "STALE_OWNER"

TASK_KEY_MAX_CHARS = 128
TTL_MIN = 1
TTL_MAX = 300
_ACQUIRE_ATTEMPTS = 5

_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")
_BAD_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Zl", "Zp", "Zs"})


class TaskLeaseError(ValueError):
    """Domain error with a stable ``code``; ``str(error)`` is only the code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _db_error() -> TaskLeaseError:
    return TaskLeaseError(DB_ERROR)


# --- validation (pure) --------------------------------------------------------


def _valid_schema(value: object) -> bool:
    return type(value) is str and _SCHEMA_PATTERN.fullmatch(value) is not None


def _valid_task_key(value: object) -> bool:
    if type(value) is not str or not 1 <= len(value) <= TASK_KEY_MAX_CHARS:
        return False
    for ch in value:
        if ch.isspace() or unicodedata.category(ch) in _BAD_CATEGORIES:
            return False
    return True


def _valid_owner_token(value: object) -> bool:
    if type(value) is not str or len(value) != 36:
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


def _valid_generation(value: object) -> bool:
    return type(value) is int and value >= 1


def _valid_ttl(value: object) -> bool:
    return type(value) is int and TTL_MIN <= value <= TTL_MAX


def _require(ok: bool) -> None:
    if not ok:
        raise TaskLeaseError(INVALID_INPUT)


def _as_utc(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise _db_error()
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _rollback(conn: Any) -> None:
    try:
        conn.rollback()
    except Exception:  # pragma: no cover - must never mask the original outcome
        pass


def _close(conn: Any) -> None:
    try:
        conn.close()
    except Exception:  # pragma: no cover
        pass


# --- repository ---------------------------------------------------------------


class TaskLeases:
    """Lease repository over one validated schema."""

    def __init__(self, *, connect_factory: Callable[[], Any], schema: str) -> None:
        if not _valid_schema(schema):
            raise TaskLeaseError(INVALID_SCHEMA)
        self._connect_factory = connect_factory
        self._schema = schema
        table = f'"{schema}"."task_leases"'
        self._select_lock = (
            "SELECT owner_token, generation, expires_at FROM "
            + table
            + " WHERE task_key = %s FOR UPDATE"
        )
        self._insert = (
            "INSERT INTO "
            + table
            + " (task_key, owner_token, generation, acquired_at, expires_at)"
            " VALUES (%s, %s::uuid, 1, 'epoch'::timestamptz, 'epoch'::timestamptz)"
            " ON CONFLICT (task_key) DO NOTHING"
        )
        # The provisional epoch values above are already expired and are replaced
        # in the same transaction by the clock read taken after the insert won.
        self._stamp = (
            "UPDATE " + table + " SET acquired_at = %s, expires_at = %s WHERE task_key = %s"
        )
        self._takeover = (
            "UPDATE "
            + table
            + " SET owner_token = %s::uuid, generation = %s, acquired_at = %s,"
            " expires_at = %s WHERE task_key = %s"
        )
        self._set_expiry = "UPDATE " + table + " SET expires_at = %s WHERE task_key = %s"

    @property
    def schema(self) -> str:
        return self._schema

    # -- public API -------------------------------------------------------------

    def acquire(self, *, task_key: str, owner_token: str, ttl_seconds: int) -> Dict[str, Any]:
        _require(_valid_task_key(task_key))
        _require(_valid_owner_token(owner_token))
        _require(_valid_ttl(ttl_seconds))
        ttl = timedelta(seconds=ttl_seconds)

        def work(cur: Any) -> Dict[str, Any]:
            for _ in range(_ACQUIRE_ATTEMPTS):
                cur.execute(self._select_lock, (task_key,))
                row = cur.fetchone()
                if row is None:
                    # No row to lock: insert it.  A conflicting uncommitted insert
                    # is waited for here, so no clock is read before this point.
                    cur.execute(self._insert, (task_key, owner_token))
                    if cur.rowcount != 1:
                        continue  # lost the race for the first row: lock it and decide again
                    decided_at = self._clock(cur)  # this transaction owns the new row now
                    cur.execute(self._stamp, (decided_at, decided_at + ttl, task_key))
                    if cur.rowcount != 1:
                        raise _db_error()
                    return self._acquired(task_key, 1, owner_token, decided_at + ttl)
                decided_at = self._clock(cur)
                generation = int(row[1])
                expires_at = _as_utc(row[2])
                if decided_at < expires_at:
                    return {
                        "status": "BUSY",
                        "task_key": task_key,
                        "generation": generation,
                        "owner_token": None,
                        "expires_at": _iso(expires_at),
                    }
                new_expiry = decided_at + ttl
                cur.execute(
                    self._takeover,
                    (owner_token, generation + 1, decided_at, new_expiry, task_key),
                )
                if cur.rowcount != 1:
                    raise _db_error()
                return self._acquired(task_key, generation + 1, owner_token, new_expiry)
            raise _db_error()

        return self._transaction(work)

    def renew(
        self, *, task_key: str, owner_token: str, generation: int, ttl_seconds: int
    ) -> Dict[str, Any]:
        _require(_valid_task_key(task_key))
        _require(_valid_owner_token(owner_token))
        _require(_valid_generation(generation))
        _require(_valid_ttl(ttl_seconds))
        ttl = timedelta(seconds=ttl_seconds)

        def work(cur: Any) -> Dict[str, Any]:
            decided_at = self._decide_exact(cur, task_key, owner_token, generation)
            if decided_at is None:
                return {"status": "STALE", "expires_at": None}
            new_expiry = decided_at + ttl
            self._write_expiry(cur, task_key, new_expiry)
            return {"status": "RENEWED", "expires_at": _iso(new_expiry)}

        return self._transaction(work)

    def finish(self, *, task_key: str, owner_token: str, generation: int) -> Dict[str, Any]:
        _require(_valid_task_key(task_key))
        _require(_valid_owner_token(owner_token))
        _require(_valid_generation(generation))

        def work(cur: Any) -> Dict[str, Any]:
            decided_at = self._decide_exact(cur, task_key, owner_token, generation)
            if decided_at is None:
                return {"status": "STALE"}
            self._write_expiry(cur, task_key, decided_at)  # expired as of this decision
            return {"status": "FINISHED"}

        return self._transaction(work)

    def assert_owned(
        self, conn: Any, *, task_key: str, owner_token: str, generation: int
    ) -> None:
        """Lock the lease row in the caller's transaction and verify ownership.

        Stages: malformed argument -> ``INVALID_INPUT`` (no SQL); missing active
        caller transaction, or any connect/SQL/clock failure -> ``DB_ERROR``
        (the transaction check happens before any SQL); a successful inspection
        that shows no active exact ownership -> ``STALE_OWNER``.  Never commits,
        rolls back or closes; on success the row lock is held until the caller's
        transaction ends.
        """
        self._validate_in_tx_args(conn, task_key, owner_token, generation)
        owned, _ = self._inspect_in_tx(conn, task_key, owner_token, generation)
        if not owned:
            raise TaskLeaseError(STALE_OWNER)

    def finish_owned_in_tx(
        self, conn: Any, *, task_key: str, owner_token: str, generation: int
    ) -> None:
        """Finish the lease inside the caller's transaction (integration bridge).

        Same stages as ``assert_owned``.  On success ``expires_at`` becomes the
        decision clock; the generation is unchanged and the row is kept.  Nothing
        is committed, rolled back or closed: the change and the row lock belong to
        the caller's transaction, so a caller rollback restores the previous lease.
        ``STALE_OWNER`` (including a lease that expired since an earlier
        ``assert_owned``) means the caller must roll back the whole transition.
        """
        self._validate_in_tx_args(conn, task_key, owner_token, generation)
        owned, decided_at = self._inspect_in_tx(conn, task_key, owner_token, generation)
        if not owned:
            raise TaskLeaseError(STALE_OWNER)
        try:
            with conn.cursor(row_factory=tuple_row) as cur:
                self._write_expiry(cur, task_key, decided_at)
        except TaskLeaseError:
            raise
        except Exception:
            raise _db_error() from None

    # -- internals ----------------------------------------------------------------

    @staticmethod
    def _validate_in_tx_args(conn: Any, task_key: Any, owner_token: Any, generation: Any) -> None:
        _require(callable(getattr(conn, "cursor", None)))
        _require(_valid_task_key(task_key))
        _require(_valid_owner_token(owner_token))
        _require(_valid_generation(generation))

    @staticmethod
    def _require_caller_transaction(conn: Any) -> None:
        """Fail closed, before any cursor or SQL, unless a transaction is already open.

        The only accepted state is ``TransactionStatus.INTRANS``: a transaction the
        caller has already started (``with conn.transaction():`` or an earlier
        statement on a non-autocommit connection) and still owns.  ``autocommit=False``
        alone is not enough: with status ``IDLE`` the first lock statement would
        begin a transaction implicitly, which this contract forbids.  ``IDLE``,
        ``ACTIVE``, ``INERROR``, ``UNKNOWN`` and an unreadable or missing status are
        all ``DB_ERROR``.  Nothing is sent to the database by this check.
        """
        try:
            autocommit = getattr(conn, "autocommit", None)
            status = getattr(getattr(conn, "info", None), "transaction_status", None)
        except Exception:
            raise _db_error() from None
        if type(autocommit) is not bool:
            raise _db_error()
        if type(status) is not TransactionStatus or status != TransactionStatus.INTRANS:
            raise _db_error()

    def _inspect_in_tx(
        self, conn: Any, task_key: str, owner_token: str, generation: int
    ) -> "tuple[bool, Optional[datetime]]":
        """Lock, read the clock once; (owned, decision clock).  Failures -> DB_ERROR."""
        self._require_caller_transaction(conn)
        try:
            with conn.cursor(row_factory=tuple_row) as cur:
                cur.execute(self._select_lock, (task_key,))
                row = cur.fetchone()
                # Defence in depth: if the lock statement left no open transaction
                # the lock is already gone, so ownership would not be fenced.
                info = getattr(conn, "info", None)
                if getattr(info, "transaction_status", None) == TransactionStatus.IDLE:
                    raise _db_error()
                decided_at = self._clock(cur)
            return self._owns(row, owner_token, generation, decided_at), decided_at
        except TaskLeaseError:
            raise
        except Exception:
            raise _db_error() from None

    @staticmethod
    def _acquired(task_key: str, generation: int, owner_token: str, expires_at: datetime) -> Dict[str, Any]:
        return {
            "status": "ACQUIRED",
            "task_key": task_key,
            "generation": generation,
            "owner_token": owner_token,
            "expires_at": _iso(expires_at),
        }

    @staticmethod
    def _clock(cur: Any) -> datetime:
        cur.execute("SELECT clock_timestamp()")
        fetched = cur.fetchone()
        if fetched is None:
            raise _db_error()
        return _as_utc(fetched[0])

    @staticmethod
    def _owns(row: Optional[Any], owner_token: str, generation: int, decided_at: datetime) -> bool:
        if row is None:
            return False
        return (
            str(row[0]) == owner_token
            and int(row[1]) == generation
            and decided_at < _as_utc(row[2])
        )

    def _decide_exact(
        self, cur: Any, task_key: str, owner_token: str, generation: int
    ) -> Optional[datetime]:
        """Lock the row, read the clock once; the decision clock if the tuple owns it."""
        cur.execute(self._select_lock, (task_key,))
        row = cur.fetchone()
        decided_at = self._clock(cur)
        return decided_at if self._owns(row, owner_token, generation, decided_at) else None

    def _write_expiry(self, cur: Any, task_key: str, expires_at: datetime) -> None:
        cur.execute(self._set_expiry, (expires_at, task_key))
        if cur.rowcount != 1:
            raise _db_error()

    def _transaction(self, work: Callable[[Any], Dict[str, Any]]) -> Dict[str, Any]:
        try:
            conn = self._connect_factory()
        except Exception:
            raise _db_error() from None
        try:
            if conn.autocommit:
                conn.autocommit = False
            with conn.cursor(row_factory=tuple_row) as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                result = work(cur)
            conn.commit()
            return result  # only reached after a confirmed commit
        except TaskLeaseError:
            _rollback(conn)
            raise
        except Exception:
            _rollback(conn)
            raise _db_error() from None
        except BaseException:
            _rollback(conn)
            raise
        finally:
            _close(conn)
