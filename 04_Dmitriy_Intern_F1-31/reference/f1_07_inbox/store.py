"""F1-07 — durable, isolated persistence of normalized inbound events.

Scope of this module (component only)
-------------------------------------
``InboxStore`` accepts an already normalized inbound event (schema
``FIRST_INBOUND_V1``) plus a trusted ``binding`` (schema
``FIRST_INBOX_BINDING_V1``) supplied by the calling code — never taken from
provider JSON — and stores it in PostgreSQL.  A replay of the same semantic
record creates no second row; the same event key carrying different content is
reported as a conflict and the original row is left untouched.

The module does **not**: read a mailbox, send mail, authenticate a provider,
normalize provider payloads (that is ``normalize_inbound`` from the Annie
F1-02 contract — not implemented and not imported here), classify replies,
import ``postgres_store``, or touch any existing runtime module.  The
``connect_factory`` is supplied by the caller, so no production database
configuration is built in and there is no hidden global connection.

Public contract
---------------
::

    class InboxStoreError(ValueError):
        code: str          # stable machine code; str(error) == code

    class InboxStore:
        def __init__(self, *, connect_factory, schema: str) -> None: ...
        def append(self, event: dict, *, binding: dict) -> dict: ...
        def get(self, *, client_id: str, mailbox_ref: str, event_key: str) -> dict | None: ...

``append`` returns, after a confirmed ``COMMIT``::

    {"status": "APPENDED" | "DUPLICATE", "inbox_id": "<canonical uuid>",
     "client_id": "...", "mailbox_ref": "...", "event_key": "<64 lowercase hex>",
     "stored_at": "<UTC ISO, +00:00>"}

``APPENDED`` means this attempt created the row; ``DUPLICATE`` means the same
semantic record already existed (same ``inbox_id`` and same ``stored_at``; no
new UUID is minted for a replay).  ``get`` returns
``{"inbox_id": str, "event": dict, "stored_at": str}`` or ``None``.

Frozen input
------------
``binding`` has exactly the keys ``schema_version`` (``FIRST_INBOX_BINDING_V1``),
``client_id``, ``runtime_campaign_id`` (canonical UUID), ``mailbox_ref``,
``sender_email``.  ``sender_email`` is the address of *our* mailbox, already
normalized.

``event`` has exactly the keys ``schema_version`` (``FIRST_INBOUND_V1``),
``client_id``, ``runtime_campaign_id``, ``mailbox_ref``, ``provider``,
``message_id``, ``thread_id``, ``sender_email``, ``recipient_email``,
``received_at``, ``body_text``, ``event_kind``, ``in_reply_to``, ``event_key``
plus the single additional permitted field ``auto_reply_headers`` (opaque
``str -> str`` data, never interpreted).  Anything else — ``lead_id``,
``auth``, ``tenant_id``, ``classifier_result``, a raw envelope — is rejected.

Validation is strict and never repairs input: identifiers keep their case, must
be non-empty, must have no leading/trailing whitespace and are bounded (512
UTF-8 bytes for the new DB fields and ``in_reply_to``, 320 for both e-mails,
65536 for ``body_text`` inclusive, 64 pairs / 16384 cumulative UTF-8 bytes for
``auto_reply_headers``).  ``received_at`` must be a timezone-aware ISO
datetime with UTC offset 0 and is normalized with ``datetime.isoformat()``
(``+00:00``) for comparison.  ``recipient_email`` must equal
``binding.sender_email`` exactly; ``client_id``, ``runtime_campaign_id`` and
``mailbox_ref`` must match the binding.  No deliverability check of its own is
added and no contact helper is imported.

A value that cannot be encoded as UTF-8 at all — a lone surrogate such as
``"\\ud800"``, which ``str.encode("utf-8")`` refuses — is an input error like any
other: ``INVALID_BINDING`` for a binding field, otherwise ``INVALID_EVENT``
(including every argument of ``get``), raised before a connection is requested.
Such a character is never deleted, replaced or normalised: an unencodable input
is refused, and every encodable string is stored byte for byte.

Semantics of replay and conflict
--------------------------------
``canonical_event`` is a deep JSON-compatible object: all event fields are
kept, ``received_at`` is normalized, ``in_reply_to`` is always present (may be
``null``) and ``auto_reply_headers`` is always present (``{}`` when absent or
empty — the two spellings are one content).  Canonical bytes::

    json.dumps(canonical_event, ensure_ascii=False, sort_keys=True,
               separators=(",", ":"), allow_nan=False).encode("utf-8")

``payload_sha256`` is the lowercase hex digest of those bytes, so JSON key
order and header order do not matter while body text, ID case, addresses,
campaign, thread, event kind, ``in_reply_to`` and header values do.

``event_key`` must equal ``sha256`` of the UTF-8 bytes of::

    json.dumps([provider, mailbox_ref, message_id], ensure_ascii=False,
               separators=(",", ":"))

It is mailbox scoped: campaign and client are not part of the key, therefore
the table carries the additional scoped uniqueness on
``(client_id, mailbox_ref, provider, message_id)`` and
``(client_id, mailbox_ref, event_key)``.  Changing the campaign for the same
message is a conflict, not a silent re-binding.

Atomicity
---------
``append`` uses one transaction on a brand new connection (``READ COMMITTED``),
starting with ``INSERT ... ON CONFLICT DO NOTHING RETURNING``; when the insert
returns nothing, the *next statement* in the same transaction selects the row
by client/mailbox/event_key — a separate statement is required because a single
CTE snapshot may miss a row committed concurrently.  Equal content →
``DUPLICATE``; different content → ``INBOX_EVENT_CONFLICT``; a conflict on the
other unique constraint with no row found by key → ``STORAGE_CONFLICT``.  A
successful receipt is only returned after ``COMMIT``.  Validation errors are
``InboxStoreError``; connection, SQL and commit errors stay ``psycopg.Error``
and never leak the DSN or a password.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, Tuple

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

__all__ = [
    "InboxStore",
    "InboxStoreError",
    "canonical_event",
    "compute_event_key",
]

# --- frozen bounds -----------------------------------------------------------

SCHEMA_PATTERN = re.compile(r"^f1_07_[a-z0-9_]+$")
SCHEMA_MAX_BYTES = 63

EVENT_SCHEMA_VERSION = "FIRST_INBOUND_V1"
BINDING_SCHEMA_VERSION = "FIRST_INBOX_BINDING_V1"

ID_MAX_BYTES = 512
BODY_MAX_BYTES = 65536
EMAIL_MAX_BYTES = 320
HEADERS_MAX_PAIRS = 64
HEADERS_MAX_BYTES = 16384

EVENT_KINDS = ("INBOUND_MESSAGE", "DELIVERY_FAILURE")
EVENT_KEY_PATTERN = re.compile(r"^[0-9a-f]{64}$")

REQUIRED_EVENT_FIELDS = (
    "schema_version",
    "client_id",
    "runtime_campaign_id",
    "mailbox_ref",
    "provider",
    "message_id",
    "thread_id",
    "sender_email",
    "recipient_email",
    "received_at",
    "body_text",
    "event_kind",
    "in_reply_to",
    "event_key",
)
OPTIONAL_EVENT_FIELDS = ("auto_reply_headers",)
REQUIRED_BINDING_FIELDS = (
    "schema_version",
    "client_id",
    "runtime_campaign_id",
    "mailbox_ref",
    "sender_email",
)

# --- stable error codes (frozen by the task) ---------------------------------

INVALID_SCHEMA = "INVALID_SCHEMA"
INVALID_EVENT = "INVALID_EVENT"
INVALID_BINDING = "INVALID_BINDING"
SCOPE_MISMATCH = "SCOPE_MISMATCH"
INVALID_EVENT_KEY = "INVALID_EVENT_KEY"
INBOX_EVENT_CONFLICT = "INBOX_EVENT_CONFLICT"
STORAGE_CONFLICT = "STORAGE_CONFLICT"


class InboxStoreError(ValueError):
    """Domain error with a stable ``code``; ``str(error)`` is only the code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


# --- small pure helpers ------------------------------------------------------


def _fail(code: str) -> "None":
    raise InboxStoreError(code)


def _is_exact_str(value: object) -> bool:
    """``bool``/``int`` are not strings: exact type identity, not isinstance."""
    return type(value) is str


def _utf8_len(value: str, *, code: str) -> int:
    """UTF-8 size of an exact ``str``; a value that cannot be stored is refused.

    Two kinds of string cannot reach this database as given, and both must come
    back as a domain error instead of a foreign one (task section 7):

    * a lone surrogate (``"\\ud800"``), which no UTF-8 encoder can
      represent, so ``str.encode("utf-8")`` raises ``UnicodeEncodeError``;
    * U+0000, which PostgreSQL cannot hold in ``text`` or in ``jsonb``: the driver
      raises ``DataError`` / ``UntranslatableCharacter`` - after the connection is
      already open and the value is half way into the statement.

    Both are refused here, before any connection is opened, with the caller's own
    code.  Nothing is deleted, replaced or normalised silently: a value is either
    stored byte for byte or rejected.
    """
    if "\x00" in value:
        _fail(code)
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        _fail(code)


def validate_schema(schema: object) -> str:
    """Accept only ``f1_07_[a-z0-9_]+`` (ASCII) of at most 63 bytes."""
    if not _is_exact_str(schema):
        _fail(INVALID_SCHEMA)
    if not SCHEMA_PATTERN.fullmatch(schema):
        _fail(INVALID_SCHEMA)
    if _utf8_len(schema, code=INVALID_SCHEMA) > SCHEMA_MAX_BYTES:
        _fail(INVALID_SCHEMA)
    return schema


def _require_bounded_string(
    value: object, *, max_bytes: int, code: str, label: str
) -> str:
    """Non-empty string, no surrounding whitespace, bounded UTF-8 size."""
    if not _is_exact_str(value):
        _fail(code)
    if value == "" or value.strip() != value:
        _fail(code)
    if _utf8_len(value, code=code) > max_bytes:
        _fail(code)
    return value


def _require_canonical_uuid(value: object, *, code: str) -> str:
    if not _is_exact_str(value):
        _fail(code)
    if len(value) != 36:
        _fail(code)
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        _fail(code)
    if str(parsed) != value:
        _fail(code)
    return value


def normalize_received_at(value: object, *, code: str = INVALID_EVENT) -> str:
    """Parse an aware ISO datetime with UTC offset 0 into ``isoformat()`` UTC.

    The raw text is checked for storability *before* it is parsed.  CPython's
    ``fromisoformat`` tolerates exactly one trailing U+0000 (it treats it as a
    C-string terminator) and hands back an ordinary datetime, so a timestamp
    carrying a byte PostgreSQL cannot store used to be accepted with the byte
    silently dropped - a repaired input where section 7 wants a rejected one.
    Other trailing junk (whitespace, letters, a second NUL) already raises
    ``ValueError``; the guard makes the whole class explicit instead of relying
    on the parser's leniency.
    """
    if not _is_exact_str(value):
        _fail(code)
    _utf8_len(value, code=code)
    # ISO-8601 also spells UTC as a trailing "Z"; CPython only learned that suffix
    # in 3.11, so it is normalised here explicitly and the accepted set does not
    # depend on the interpreter the component happens to run on.  This is a
    # documented spelling equivalence, not a repair: no character is dropped.
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except (ValueError, TypeError):
        _fail(code)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    if parsed.utcoffset() != timedelta(0):
        _fail(code)
    return parsed.astimezone(timezone.utc).isoformat()


def _normalize_headers(value: object, *, code: str) -> Dict[str, str]:
    # An absent ``auto_reply_headers`` and ``{}`` are one and the same content
    # (task section 5).  An explicitly present ``null`` is neither: the field is
    # declared as a mapping of strings, so it is refused instead of being
    # silently turned into an empty dictionary.
    if type(value) is not dict:
        _fail(code)
    if len(value) > HEADERS_MAX_PAIRS:
        _fail(code)
    total = 0
    normalized: Dict[str, str] = {}
    for key, item in value.items():
        if not _is_exact_str(key) or not _is_exact_str(item):
            _fail(code)
        # A header name or value that cannot be encoded as UTF-8 (lone
        # surrogate) is a rejected input, never a silently dropped header.
        total += _utf8_len(key, code=code) + _utf8_len(item, code=code)
        normalized[key] = item
    if total > HEADERS_MAX_BYTES:
        _fail(code)
    return normalized


def compute_event_key(provider: str, mailbox_ref: str, message_id: str) -> str:
    """sha256 of ``json.dumps([provider, mailbox_ref, message_id], …)`` bytes."""
    payload = json.dumps(
        [provider, mailbox_ref, message_id], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _canonical_bytes(canonical: Dict[str, Any]) -> bytes:
    return json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def payload_sha256(canonical: Dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(canonical)).hexdigest()


def _canonical_json_text(canonical: Dict[str, Any]) -> str:
    """Canonical text handed to the ``jsonb`` parameter (readable, sorted)."""
    return _canonical_bytes(canonical).decode("utf-8")


def canonical_event(event: object, *, binding: object) -> Dict[str, Any]:
    """Validate ``event``/``binding`` and return the canonical event object.

    Validation order: binding → event → scope.  Raises ``InboxStoreError`` with
    ``INVALID_BINDING``, ``INVALID_EVENT`` or ``SCOPE_MISMATCH``.
    """
    bound = _validate_binding(binding)
    canonical, _ = _validate_event(event, bound)
    return canonical


def _validate_binding(binding: object) -> Dict[str, str]:
    if type(binding) is not dict:
        _fail(INVALID_BINDING)
    if set(binding) != set(REQUIRED_BINDING_FIELDS):
        _fail(INVALID_BINDING)
    if binding["schema_version"] != BINDING_SCHEMA_VERSION:
        _fail(INVALID_BINDING)
    client_id = _require_bounded_string(
        binding["client_id"], max_bytes=ID_MAX_BYTES, code=INVALID_BINDING, label="client_id"
    )
    mailbox_ref = _require_bounded_string(
        binding["mailbox_ref"], max_bytes=ID_MAX_BYTES, code=INVALID_BINDING, label="mailbox_ref"
    )
    campaign = _require_canonical_uuid(
        binding["runtime_campaign_id"], code=INVALID_BINDING
    )
    sender_email = _require_bounded_string(
        binding["sender_email"],
        max_bytes=EMAIL_MAX_BYTES,
        code=INVALID_BINDING,
        label="sender_email",
    )
    return {
        "client_id": client_id,
        "runtime_campaign_id": campaign,
        "mailbox_ref": mailbox_ref,
        "sender_email": sender_email,
    }


def _validate_event(event: object, bound: Dict[str, str]) -> Tuple[Dict[str, Any], str]:
    if type(event) is not dict:
        _fail(INVALID_EVENT)
    keys = set(event)
    if not set(REQUIRED_EVENT_FIELDS) <= keys:
        _fail(INVALID_EVENT)
    if keys - set(REQUIRED_EVENT_FIELDS) - set(OPTIONAL_EVENT_FIELDS):
        _fail(INVALID_EVENT)
    if event["schema_version"] != EVENT_SCHEMA_VERSION:
        _fail(INVALID_EVENT)

    client_id = _require_bounded_string(
        event["client_id"], max_bytes=ID_MAX_BYTES, code=INVALID_EVENT, label="client_id"
    )
    mailbox_ref = _require_bounded_string(
        event["mailbox_ref"], max_bytes=ID_MAX_BYTES, code=INVALID_EVENT, label="mailbox_ref"
    )
    provider = _require_bounded_string(
        event["provider"], max_bytes=ID_MAX_BYTES, code=INVALID_EVENT, label="provider"
    )
    message_id = _require_bounded_string(
        event["message_id"], max_bytes=ID_MAX_BYTES, code=INVALID_EVENT, label="message_id"
    )
    thread_id = _require_bounded_string(
        event["thread_id"], max_bytes=ID_MAX_BYTES, code=INVALID_EVENT, label="thread_id"
    )
    campaign = _require_bounded_string(
        event["runtime_campaign_id"],
        max_bytes=ID_MAX_BYTES,
        code=INVALID_EVENT,
        label="runtime_campaign_id",
    )
    sender_email = _require_bounded_string(
        event["sender_email"],
        max_bytes=EMAIL_MAX_BYTES,
        code=INVALID_EVENT,
        label="sender_email",
    )
    recipient_email = _require_bounded_string(
        event["recipient_email"],
        max_bytes=EMAIL_MAX_BYTES,
        code=INVALID_EVENT,
        label="recipient_email",
    )

    if not _is_exact_str(event["body_text"]):
        _fail(INVALID_EVENT)
    body_text = event["body_text"]
    if body_text.strip() == "":
        _fail(INVALID_EVENT)
    if _utf8_len(body_text, code=INVALID_EVENT) > BODY_MAX_BYTES:
        _fail(INVALID_EVENT)

    if event["event_kind"] not in EVENT_KINDS:
        _fail(INVALID_EVENT)

    in_reply_to = event["in_reply_to"]
    if in_reply_to is not None:
        in_reply_to = _require_bounded_string(
            in_reply_to, max_bytes=ID_MAX_BYTES, code=INVALID_EVENT, label="in_reply_to"
        )

    received_at = normalize_received_at(event["received_at"])

    event_key = event["event_key"]
    if not _is_exact_str(event_key) or not EVENT_KEY_PATTERN.fullmatch(event_key):
        _fail(INVALID_EVENT_KEY)
    if event_key != compute_event_key(provider, mailbox_ref, message_id):
        _fail(INVALID_EVENT_KEY)

    if client_id != bound["client_id"]:
        _fail(SCOPE_MISMATCH)
    if mailbox_ref != bound["mailbox_ref"]:
        _fail(SCOPE_MISMATCH)
    if campaign != bound["runtime_campaign_id"]:
        _fail(SCOPE_MISMATCH)
    if recipient_email != bound["sender_email"]:
        _fail(SCOPE_MISMATCH)

    headers = (
        _normalize_headers(event["auto_reply_headers"], code=INVALID_EVENT)
        if "auto_reply_headers" in event
        else {}
    )

    canonical: Dict[str, Any] = {
        "schema_version": EVENT_SCHEMA_VERSION,
        "client_id": client_id,
        "runtime_campaign_id": campaign,
        "mailbox_ref": mailbox_ref,
        "provider": provider,
        "message_id": message_id,
        "thread_id": thread_id,
        "sender_email": sender_email,
        "recipient_email": recipient_email,
        "received_at": received_at,
        "body_text": body_text,
        "event_kind": event["event_kind"],
        "in_reply_to": in_reply_to,
        "event_key": event_key,
        "auto_reply_headers": headers,
    }
    return canonical, event_key


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _read_only(conn: Any) -> None:
    """End the implicit read transaction without holding locks."""
    try:
        conn.rollback()
    except Exception:  # pragma: no cover - defensive, must never mask the result
        pass


# --- store -------------------------------------------------------------------

_INSERT_SQL = sql.SQL(
    "INSERT INTO {table} (inbox_id, client_id, mailbox_ref, provider, message_id,"
    " event_key, payload_sha256, normalized_event)"
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    " ON CONFLICT DO NOTHING"
    " RETURNING inbox_id, stored_at"
)

_SELECT_BY_KEY_SQL = sql.SQL(
    "SELECT inbox_id, stored_at, provider, message_id, payload_sha256, normalized_event"
    " FROM {table} WHERE client_id = %s AND mailbox_ref = %s AND event_key = %s"
)

_SET_READ_COMMITTED_SQL = sql.SQL("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")


class InboxStore:
    """Durable append/get of normalized inbound events in one ``f1_07_*`` schema."""

    def __init__(self, *, connect_factory: Callable[[], Any], schema: str) -> None:
        # The constructor validates only the schema: no table creation, no DB
        # connection, no built-in production configuration.
        self._schema = validate_schema(schema)
        self._connect_factory = connect_factory
        self._table = sql.Identifier(self._schema, "inbox_events")

    @property
    def schema(self) -> str:
        return self._schema

    def append(self, event: dict, *, binding: dict) -> Dict[str, Any]:
        canonical, event_key = self._validated(event, binding)
        payload_sha = payload_sha256(canonical)
        inbox_id = str(uuid.uuid4())
        client_id = canonical["client_id"]
        mailbox_ref = canonical["mailbox_ref"]
        provider = canonical["provider"]
        message_id = canonical["message_id"]

        conn = self._connect_factory()
        try:
            # The whole operation is one transaction: the isolation statement, the
            # INSERT and the following SELECT must share it (task section 4).  A
            # factory that hands back an autocommit connection would silently break
            # that - every statement would commit on its own and ``commit()`` would
            # be a no-op - so the precondition is enforced here instead of only
            # documented.  psycopg allows the switch before a transaction starts.
            if conn.autocommit:
                conn.autocommit = False
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(_SET_READ_COMMITTED_SQL)
                cur.execute(
                    _INSERT_SQL.format(table=self._table),
                    (
                        inbox_id,
                        client_id,
                        mailbox_ref,
                        provider,
                        message_id,
                        event_key,
                        payload_sha,
                        Jsonb(canonical, dumps=_canonical_json_text),
                    ),
                )
                inserted = cur.fetchone()
                if inserted is None:
                    # Separate next statement in the same transaction: it sees
                    # a row committed concurrently in READ COMMITTED.
                    cur.execute(
                        _SELECT_BY_KEY_SQL.format(table=self._table),
                        (client_id, mailbox_ref, event_key),
                    )
                    existing = cur.fetchone()
                    if existing is None:
                        # Conflict happened on the other unique constraint
                        # (or on the primary key): never a fake DUPLICATE.
                        raise InboxStoreError(STORAGE_CONFLICT)
                    if not _same_record(existing, canonical, payload_sha):
                        raise InboxStoreError(INBOX_EVENT_CONFLICT)
                    status = "DUPLICATE"
                    result_inbox_id = str(existing["inbox_id"])
                    stored_at = _iso_utc(existing["stored_at"])
                else:
                    status = "APPENDED"
                    result_inbox_id = str(inserted["inbox_id"])
                    stored_at = _iso_utc(inserted["stored_at"])
            conn.commit()
        except BaseException:
            _read_only(conn)
            raise
        finally:
            conn.close()

        return {
            "status": status,
            "inbox_id": result_inbox_id,
            "client_id": client_id,
            "mailbox_ref": mailbox_ref,
            "event_key": event_key,
            "stored_at": stored_at,
        }

    def get(
        self, *, client_id: str, mailbox_ref: str, event_key: str
    ) -> Optional[Dict[str, Any]]:
        # Bad arguments must be rejected before any connection is opened.
        client = _require_bounded_string(
            client_id, max_bytes=ID_MAX_BYTES, code=INVALID_EVENT, label="client_id"
        )
        mailbox = _require_bounded_string(
            mailbox_ref, max_bytes=ID_MAX_BYTES, code=INVALID_EVENT, label="mailbox_ref"
        )
        if not _is_exact_str(event_key) or not EVENT_KEY_PATTERN.fullmatch(event_key):
            _fail(INVALID_EVENT_KEY)

        conn = self._connect_factory()
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    _SELECT_BY_KEY_SQL.format(table=self._table),
                    (client, mailbox, event_key),
                )
                row = cur.fetchone()
            _read_only(conn)
        finally:
            conn.close()

        if row is None:
            return None
        return {
            "inbox_id": str(row["inbox_id"]),
            "event": row["normalized_event"],
            "stored_at": _iso_utc(row["stored_at"]),
        }

    # -- internals ------------------------------------------------------------

    @staticmethod
    def _validated(event: object, binding: object) -> Tuple[Dict[str, Any], str]:
        bound = _validate_binding(binding)
        return _validate_event(event, bound)


def _same_record(
    existing: Dict[str, Any], canonical: Dict[str, Any], payload_sha: str
) -> bool:
    """Compare hash *and* JSON content of the stored canonical event."""
    if existing["provider"] != canonical["provider"]:
        return False
    if existing["message_id"] != canonical["message_id"]:
        return False
    if existing["payload_sha256"] != payload_sha:
        return False
    stored = existing["normalized_event"]
    if type(stored) is not dict:
        return False
    try:
        return _canonical_bytes(stored) == _canonical_bytes(canonical)
    except (ValueError, TypeError):
        return False
