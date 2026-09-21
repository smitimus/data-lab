"""
Verisim source readiness gate
=============================

Why this exists
---------------
Provisioning (``proxmox-restore-guide.md`` » "Step 5: Enable Airflow DAGs")
unpauses the grocery DAGs as soon as the stack is up. ``grocery_complete_pipeline``
runs every 6 hours with ``catchup=False``, so unpausing immediately starts a run —
while ``verisim-grocery`` is still self-bootstrapping its source DB
(bootstrap → seed → 30-day backfill → realtime).

The ingest DAG creates each raw table lazily from the *first non-empty API page*
(``grocery_ingest_api._ensure_table``). While the source is still bootstrapping the
late-arriving event endpoints return ``total: 0``, no raw table is created, and dbt
staging then fails with ``relation "raw_pos.loyalty_point_transactions" does not
exist`` — the first-run failure documented in ``AGENTS.md``. The failure is raced by
design, and the wasted pass costs roughly an hour on a fresh host.

This module turns that race into a wait. ``grocery_complete_pipeline`` starts with the
``wait_for_verisim_readiness`` sensor, which pokes the source until it is demonstrably
serving data and only then releases ``ingest``.

What "ready" means
------------------
Ready == **the source can satisfy the ingest DAG**. Concretely, all of:

1. ``GET /health``                              → ``details[<industry>] == "healthy"``
   (the API process is up)
2. ``GET /<industry>/status``                   → ``state.is_running`` is true and
                                                   ``state.mode == "realtime"``
   (the generator finished bootstrap *and* its 30-day backfill; ``stopped`` and
   ``backfill`` are both "not ready")
3. ``GET /<industry>/stats/backfill-progress``  → ``in_progress`` is false
   (belt-and-braces with (2): the generator also reports targeted gap backfills here)
4. **every** source endpoint ``grocery_ingest_api.TABLE_CONFIGS`` loads (32 tables
   across 9 schemas) answers 200 — and, for a table the EDW does not hold yet, answers
   with at least one row.

Checks 1-3 run first and cost three requests; while they fail, check 4 is skipped for
that poke, so a bootstrapping generator is not probed 32 times a minute.

Why the probe list is derived instead of listed here (t_17927141, 2026-09-21)
----------------------------------------------------------------------------
This module used to carry its own ``CRITICAL_PROBES`` — a hand-maintained pair of
"critical" endpoints. A hand-maintained subset can only ever be as current as the last
person who edited it, and it went stale the moment the DAG grew: t_2382c671 (returns)
and t_24fae529 (the online order channel) added five ingest tasks whose endpoints the
source image on dev did not serve at all. The gate passed — its two endpoints were
populated — then five ingest tasks failed with ``404 Client Error`` on attempt 1 and
again on 2, and the run ended in ``verify_raw_vs_source`` with

    source relation online.orders is unreadable: relation "online.orders" does not exist

which is an accurate message, delivered an hour late, about exactly the condition this
gate exists to catch. The probe list is now built from ``grocery_ingest_api``'s own
table registry (``TABLE_CONFIGS`` + ``SOURCE_RELATIONS``), so the gate and the load
cannot drift: a table added to the DAG joins the gate in the same commit, and the
failure reads "waiting for source relation online.orders" at the sensor instead.

Failure-mode rules (deliberate, see ``evaluate_readiness``)
-----------------------------------------------------------
* An endpoint the source does not serve (**HTTP 404**) → **not ready**, named by its
  source relation. The load cannot succeed, so neither may the gate.
* Empty rows, an unreachable endpoint, a 5xx, or a 200 carrying no row count →
  **not ready when the corresponding raw table does not exist yet** (the ingest would
  create nothing and dbt staging would fail on a missing relation — the original
  first-run failure), and a **warning** when it does (that table is already loaded; a
  0-row page is then a fact about the source that waiting cannot change, and blocking
  on it would stall the pipeline forever). The raw-table inventory is read once per
  poke from the EDW; if that read fails the gate does **not** block on emptiness,
  because the gate's own inability to probe must never be what halts the pipeline.
* A **4xx other than 404** (e.g. a required query param this module forgot) → **not a
  block**. It means *this module asked wrongly*, which is a defect here, not evidence
  about the source. It is logged loudly and reported in the verdict reason, and the
  generator-state checks still gate. Without this rule a probe bug would silently
  deadlock the pipeline forever — the same class of bug this module exists to fix.
  (``/pos/transactions`` requires ``start_dt``/``end_dt``; a bare ``limit`` earns an
  HTTP 422 on the live API — verified 2026-09-21.)

Readiness stays *state*-based, not *volume*-based: it never waits for a stable row
count, because in steady state the generator writes continuously and a "no growth for
N seconds" rule would deadlock every scheduled run. The data probes ask for one row
(``limit=1``), so the whole check stays cheap on every poke.

Operational notes
-----------------
* Steady state costs one poke: the three state requests plus 32 ``limit=1`` probes
  (a few seconds on the shared Docker network).
* ``mode="reschedule"`` keeps the worker slot free while waiting; the sensor is only
  ever queued, so it cannot starve the other DAGs.
* ``READINESS_TIMEOUT_MIN`` bounds the wait. If the source never becomes ready the run
  fails *loudly, in the sensor*, with the last probe reason in the task log — listing
  the source relation(s) it was waiting for — rather than failing later inside dbt with
  an opaque "relation does not exist". The next scheduled interval retries by itself.
* All values are env-overridable so a fresh host can be tuned without a code change:
  ``VERISIM_READINESS_TIMEOUT_MIN``, ``VERISIM_READINESS_POKE_S``,
  ``VERISIM_PROBE_TIMEOUT_S``, ``VERISIM_READINESS_LOOKBACK_DAYS``,
  ``VERISIM_API_URL`` (source address; defaults to the ``verisim-grocery`` Docker
  service on the shared network, never the host ``IP``), ``VERISIM_INDUSTRY``.

CLI (used by the e2e harness; must run inside an airflow container that shares the
source's network — `airflow-worker`, not the api server, which does not join it):
    docker exec airflow-worker python /opt/airflow/dags/verisim_readiness.py --wait
Exit code 0 = ready, 1 = not ready / timed out.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import requests

log = logging.getLogger(__name__)

INDUSTRY = os.getenv("VERISIM_INDUSTRY", "grocery")

# Source address — the Verisim instance that belongs to THIS stack.
#
# Resolved by Docker service name over the shared network, deliberately NOT from the
# host's `IP` env var, and matching `grocery_ingest_api.API_BASE` (same convention,
# same default). `IP` is baked into the container environment when the container is
# created; when it goes stale the probe does not fail, it asks a *different*
# instance for its readiness (t_05b48b69, 2026-09-21: a fresh dev instance still
# carried IP=192.168.1.7 from the host being replaced and ingested that host's
# dataset). A gate that reports READY for another instance's source is worse than no
# gate, so this module never falls back to an IP.
#
# `VERISIM_API_URL` overrides it (used by the e2e harness and the tests).
API_BASE = os.getenv("VERISIM_API_URL") or "http://verisim-grocery:8000"

REQUEST_TIMEOUT_S = float(os.getenv("VERISIM_PROBE_TIMEOUT_S", "10"))
POKE_INTERVAL_S = int(os.getenv("VERISIM_READINESS_POKE_S", "60"))
# 2h: comfortably longer than a fresh-host 30-day backfill, short enough that a
# permanently-unavailable source surfaces inside a single 6-hour schedule interval.
READINESS_TIMEOUT_MIN = int(os.getenv("VERISIM_READINESS_TIMEOUT_MIN", "120"))

# Modes in which the source is serving complete data. "backfill" = still building
# history, "stopped" = generator down; neither is safe to ingest from.
READY_MODES = ("realtime",)

# Rolling window for date-bounded probes. Matches the ingest DAG's incremental
# fallback (grocery_ingest_api.INCREMENTAL_FALLBACK_DAYS = 365) so the probe asks for
# the same span the first ingest pass will read.
LOOKBACK_DAYS = int(os.getenv("VERISIM_READINESS_LOOKBACK_DAYS", "365"))

# The DAG whose table registry defines readiness. It lives beside this module in
# `dags/`; the registry is imported lazily (see _ingest_registry).
INGEST_MODULE = "grocery_ingest_api"

# How many offenders a verdict reason names before it summarises the rest. A fresh
# host can be missing every table at once; the reason must stay readable in a task log.
REASON_LIST_LIMIT = 5


# ---------------------------------------------------------------------------
# What must be ready — derived from the ingest DAG, never listed here
# ---------------------------------------------------------------------------

# `dags/` of the Airflow deployment this module ships in. Made explicit because this
# file is imported from three different places (Airflow's DAG parse, the CLI entry
# point, the unit tests) and only the first of them is guaranteed to have `dags/` on
# sys.path.
_DAGS_DIR = os.path.dirname(os.path.abspath(__file__))

_registry: tuple[list, dict] | None = None


def _ingest_registry() -> tuple[list, dict]:
    """``(TABLE_CONFIGS, SOURCE_RELATIONS)`` from the ingest DAG. Imported once.

    Lazy on purpose: this module is imported at DAG-parse time by
    ``grocery_complete_pipeline``, and a hard import failure here would remove every
    pipeline DAG from the Airflow UI. Instead a failure is recorded in the evidence
    and turned into a *verdict* ("cannot verify the source against the ingest
    registry"), which the sensor reports and the timeout bounds — the same shape as
    every other failure this gate has.
    """
    global _registry
    if _registry is None:
        if _DAGS_DIR not in sys.path:
            sys.path.insert(0, _DAGS_DIR)
        import importlib

        module = importlib.import_module(INGEST_MODULE)
        configs = list(module.TABLE_CONFIGS)
        relations = dict(module.SOURCE_RELATIONS)
        unmapped = sorted(c[0] for c in configs if c[0] not in relations)
        if unmapped:
            raise ValueError(
                f"{INGEST_MODULE}.SOURCE_RELATIONS has no source relation for: "
                + ", ".join(unmapped)
            )
        _registry = (configs, relations)
    return _registry


def _param_value(moment: datetime, param_name: str | None) -> str:
    """Format a window boundary the way the ingest does (``ingest_table._fmt``).

    Endpoints whose parameter is named ``*_date`` want ``YYYY-MM-DD``; the rest take a
    full ISO timestamp. Mirroring the load's own formatting means the probe asks for
    exactly the request the ingest is about to make.
    """
    iso = moment.isoformat()
    return iso[:10] if param_name and param_name.endswith("_date") else iso


def required_probes(now: datetime | None = None) -> list[tuple[str, dict, str, str, str]]:
    """Every source relation this DAG's ingest depends on, as probe descriptors.

    One entry per ``grocery_ingest_api.TABLE_CONFIGS`` row:
    ``(api_path, query_params, source_relation, raw_relation, task_id)``.

    ``query_params`` mirrors what ``ingest_table`` sends for that row: ``limit=1``, plus
    the rolling date window for the endpoints that declare one (the incremental ones —
    a bare ``limit`` earns a HTTP 422 from those).
    """
    configs, relations = _ingest_registry()
    end = now or datetime.now(timezone.utc)
    start = end - timedelta(days=LOOKBACK_DAYS)

    probes: list[tuple[str, dict, str, str, str]] = []
    for (task_id, api_path, raw_schema, raw_table, _pk_col, _strategy, _watermark,
         api_start_param, api_end_param) in configs:
        params: dict[str, Any] = {"limit": 1}
        if api_start_param:
            params[api_start_param] = _param_value(start, api_start_param)
            params[api_end_param] = _param_value(end, api_end_param)
        probes.append((
            api_path,
            params,
            relations[task_id],
            f"{raw_schema}.{raw_table}",
            task_id,
        ))
    return probes


def _raw_table_inventory() -> tuple[set[str] | None, str | None]:
    """``{'raw_pos.transactions', ...}`` — the raw tables the EDW already holds.

    ``(None, reason)`` when the EDW cannot be read. Callers must then treat every table
    as present: a gate that blocks because *it* cannot look something up would be the
    deadlock this module has a rule against (see the module docstring).
    """
    try:
        import psycopg2  # imported here so the module loads without a DB driver

        from grocery_ingest_api import EDW_CONN
    except Exception as exc:  # noqa: BLE001 — any import failure means "unknown"
        return None, f"{type(exc).__name__}: {exc}"

    try:
        conn = psycopg2.connect(**EDW_CONN)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_schema || '.' || table_name "
                    "FROM information_schema.tables "
                    "WHERE table_schema LIKE 'raw\\_%'"
                )
                return {row[0] for row in cur.fetchall()}, None
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — EDW unreachable is not source unreadiness
        return None, f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# Probe (I/O)
# ---------------------------------------------------------------------------

def _get(path: str, params: Mapping[str, Any] | None = None,
         timeout: float | None = None) -> tuple[int | None, Any, str | None]:
    """GET ``path`` → ``(status_code, json_body, error)``.

    ``status_code`` is None for a transport-level failure; ``json_body`` is None when
    the response was not JSON. The body may be a dict *or* a bare list: the source
    serves some endpoints as ``{"data": [...], "total": n}`` and others as ``[...]``
    (``/hr/locations``, ``/pos/coupons``), and the ingest handles both.
    """
    url = f"{API_BASE}{path}"
    try:
        resp = requests.get(url, params=dict(params or {}),
                            timeout=timeout or REQUEST_TIMEOUT_S)
    except requests.RequestException as exc:
        return None, None, str(exc)
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    if not isinstance(payload, (dict, list)):
        payload = None
    return resp.status_code, payload, None


def _json(path: str, params: Mapping[str, Any] | None = None,
          timeout: float | None = None) -> dict | None:
    """``_get`` for probes where any failure simply means "cannot prove readiness"."""
    status, payload, error = _get(path, params, timeout)
    if status != 200 or not isinstance(payload, dict):
        log.warning("[readiness] %s%s failed (status=%s%s)",
                    API_BASE, path, status, f", {error}" if error else "")
        return None
    return payload


def _total_of(payload: Any) -> int | None:
    """Row count advertised by an endpoint, or None when it advertises none.

    ``{"total": n}`` for the paginated endpoints, the list length for the endpoints
    that answer with a bare array (the ingest reads them the same way, via
    ``grocery_ingest_api._fetch_pages``), and ``len(data)`` for a payload that carries
    ``data`` without a ``total``.
    """
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        total = payload.get("total")
        if isinstance(total, int):
            return total
        data = payload.get("data")
        if isinstance(data, list):
            return len(data)
    return None


def collect_evidence(industry: str = INDUSTRY, timeout: float | None = None,
                     now: datetime | None = None) -> dict:
    """Gather one readiness snapshot. Pure I/O; the verdict is in evaluate_readiness."""
    evidence: dict[str, Any] = {
        "health": _json("/health", timeout=timeout),
        "status": _json(f"/{industry}/status", timeout=timeout),
        "backfill": _json(f"/{industry}/stats/backfill-progress", timeout=timeout),
        "probes": {},
        "probe_error": None,
        "probes_skipped": None,
        "raw_tables": None,
        "raw_tables_error": None,
    }

    try:
        probes = required_probes(now=now)
    except Exception as exc:  # noqa: BLE001 — verdict, not traceback (see _ingest_registry)
        evidence["probe_error"] = f"{type(exc).__name__}: {exc}"
        log.error("[readiness] cannot derive the required source relations from %s: %s",
                  INGEST_MODULE, exc)
        return evidence

    # Cheap state checks first: while the generator is still bootstrapping there is
    # nothing to learn from 32 data probes, and poking them every minute would add load
    # to a source that is busy building its history.
    state_reason = _state_reason(evidence, industry)
    if state_reason is not None:
        evidence["probes_skipped"] = state_reason
        log.info("[readiness] generator state not ready (%s) — skipping this poke's %d "
                 "source-relation probe(s)", state_reason, len(probes))
        return evidence

    for api_path, params, relation, raw_relation, task_id in probes:
        status, payload, error = _get(api_path, params, timeout)
        total = _total_of(payload) if status == 200 else None
        evidence["probes"][api_path] = {
            "task_id": task_id,
            "params": params,
            "status": status,
            "total": total,
            "relation": relation,
            "raw_table": raw_relation,
        }
        if status != 200:
            log.warning("[readiness] %s%s (%s) -> status=%s%s", API_BASE, api_path,
                        relation, status, f" ({error})" if error else "")

    evidence["raw_tables"], evidence["raw_tables_error"] = _raw_table_inventory()
    if evidence["raw_tables_error"]:
        log.warning("[readiness] cannot read the EDW raw-table inventory (%s) — an empty "
                    "source endpoint will be reported, not treated as unreadiness",
                    evidence["raw_tables_error"])

    return evidence


# ---------------------------------------------------------------------------
# Verdict (pure — no I/O, unit-testable)
# ---------------------------------------------------------------------------

def _state_reason(evidence: Mapping[str, Any], industry: str = INDUSTRY) -> str | None:
    """Reason the *generator state* is not ready, or None when it is.

    Split out because it is both the first half of the verdict and the gate on whether
    the 32 data probes are worth spending in this poke.
    """
    health = evidence.get("health")
    if health is None:
        return "verisim API /health unreachable or non-200"
    if health.get("status") != "healthy":
        return f"verisim API /health status={health.get('status')!r}"
    details = health.get("details") or {}
    if details.get(industry) != "healthy":
        return f"verisim API reports {industry}={details.get(industry)!r}"

    status = evidence.get("status")
    if status is None:
        return f"/{industry}/status unreachable or non-200"
    state = status.get("state") or {}
    if not state:
        return f"/{industry}/status returned no generator state"

    mode = state.get("mode")
    if not state.get("is_running"):
        return (
            f"generator not running (mode={mode!r}, is_running={state.get('is_running')!r})"
        )
    if mode not in READY_MODES:
        progress = evidence.get("backfill") or {}
        extra = ""
        if mode == "backfill" and "pct_complete" in progress:
            extra = (
                f" pct_complete={progress['pct_complete']}%"
                f" days_remaining={progress.get('days_remaining')}"
            )
        return f"generator mode={mode!r} — verisim still bootstrapping{extra}"

    backfill = evidence.get("backfill")
    if backfill is None:
        return f"/{industry}/stats/backfill-progress unreachable or non-200"
    if backfill.get("in_progress"):
        return (
            "backfill in progress "
            f"(pct_complete={backfill.get('pct_complete')}%, "
            f"days_remaining={backfill.get('days_remaining')})"
        )
    return None


def _capped(items: list[str]) -> str:
    shown = items[:REASON_LIST_LIMIT]
    extra = f" (+{len(items) - REASON_LIST_LIMIT} more)" if len(items) > REASON_LIST_LIMIT else ""
    return ", ".join(shown) + extra


def evaluate_readiness(evidence: Mapping[str, Any],
                       industry: str = INDUSTRY) -> tuple[bool, str]:
    """Decide readiness from an evidence dict, returning ``(ready, reason)``.

    ``reason`` always explains the verdict and carries the evidence behind it, so the
    sensor's task log is self-diagnosing (which check failed, and with what — named by
    source relation, so the next reader does not have to translate an API path into a
    table).
    """
    state_reason = _state_reason(evidence, industry)
    if state_reason is not None:
        return False, state_reason

    probe_error = evidence.get("probe_error")
    if probe_error:
        return False, (
            "cannot verify the source against the ingest registry "
            f"({INGEST_MODULE}): {probe_error}"
        )

    probes = evidence.get("probes") or {}
    if not probes:
        # A ready generator with no probes means the registry could not be read or is
        # empty. Refuse to claim readiness rather than pass on no evidence; the sensor
        # timeout bounds it and reports this line.
        return False, (
            "no source-relation probes were collected for a ready generator — refusing "
            f"to declare readiness without checking {INGEST_MODULE}.TABLE_CONFIGS"
        )

    raw_tables = evidence.get("raw_tables")

    missing: list[str] = []
    unreachable: list[str] = []
    empty_new: list[str] = []
    empty_loaded: list[str] = []
    rejected: list[str] = []
    populated: list[str] = []

    for api_path, probe in sorted(probes.items()):
        relation = probe.get("relation") or api_path
        raw_table = probe.get("raw_table")
        probe_status = probe.get("status")
        total = probe.get("total")

        if probe_status == 404:
            # The source does not serve this endpoint at all. The load will fail here
            # (HTTPError on the same request), so the gate must not pass. This is the
            # t_17927141 case: named by relation, so the task log says which table.
            missing.append(f"source relation {relation} not served ({api_path} → HTTP 404)")
        elif probe_status is not None and 400 <= probe_status < 500:
            rejected.append(f"{api_path} (HTTP {probe_status})")
        elif probe_status != 200:
            unreachable.append(f"{relation} ({api_path} → {probe_status!r})")
        elif total is None:
            unreachable.append(f"{relation} ({api_path} → 200 with no row count)")
        elif total <= 0:
            # Empty + no raw table yet = the ingest creates nothing and dbt staging
            # fails on a missing relation (the original first-run failure). Empty +
            # table already loaded = a fact about the source; waiting cannot change it
            # and blocking on it would stall the pipeline, so it is reported instead.
            # (raw_tables is None when the EDW could not be read → do not block.)
            if raw_tables is not None and raw_table not in raw_tables:
                empty_new.append(f"source relation {relation} serving no rows yet "
                                 f"({api_path} → total=0, {raw_table} not loaded yet)")
            else:
                empty_loaded.append(f"{relation} ({api_path} → total=0)")
        else:
            populated.append(f"{relation}={total}")

    # A 4xx that is not a 404 is *this module* asking wrongly, not the source being
    # unready. Never block on it — the generator-state checks above already gate — but
    # say so in both the task log and the verdict reason.
    rejected_note = ""
    if rejected:
        detail = ", ".join(rejected)
        rejected_note = ("; probe(s) rejected by the API, not treated as unreadiness "
                         f"(probe params need fixing): {detail}")
        log.warning("[readiness] probe(s) rejected by the API, not treated as "
                    "unreadiness (probe params need fixing): %s", detail)

    empty_note = ""
    if empty_loaded:
        detail = ", ".join(empty_loaded)
        empty_note = f"; already-loaded table(s) serving no rows (not blocking): {detail}"
        log.warning("[readiness] %d source relation(s) serving no rows but already "
                    "loaded into the EDW (not blocking): %s", len(empty_loaded), detail)

    problems: list[str] = []
    if missing:
        problems.append(
            f"waiting for {len(missing)} of {len(probes)} source relation(s) this DAG "
            f"ingests that the source does not serve: {_capped(missing)}"
        )
    if unreachable:
        problems.append(
            f"{len(unreachable)} source endpoint(s) unreachable: {_capped(unreachable)}"
        )
    if empty_new:
        problems.append(
            f"waiting for {len(empty_new)} source relation(s) serving no rows yet (their "
            f"raw table would never be created, so dbt staging would fail on a missing "
            f"relation): {_capped(empty_new)}"
        )
    if problems:
        return False, "; ".join(problems) + rejected_note

    return True, (
        f"mode=realtime, backfill complete — all {len(populated)} source relations this "
        f"DAG ingests are served and non-empty "
        f"({', '.join(rel.split('=')[0] for rel in populated[:3])}, …)"
        + empty_note + rejected_note
    )


# ---------------------------------------------------------------------------
# Sensor / CLI entry points
# ---------------------------------------------------------------------------

def check(industry: str = INDUSTRY, timeout: float | None = None) -> tuple[bool, str]:
    """One probe + verdict, logged so the sensor's task log carries the reason."""
    ready, reason = evaluate_readiness(collect_evidence(industry, timeout=timeout), industry)
    if ready:
        log.info("[readiness] READY — %s", reason)
    else:
        log.warning("[readiness] NOT READY — %s", reason)
    return ready, reason


def is_ready() -> bool:
    """``PythonSensor`` callable: true once verisim is serving data."""
    return check()[0]


def wait_until_ready(timeout_min: int = READINESS_TIMEOUT_MIN,
                     interval_s: int = POKE_INTERVAL_S,
                     industry: str = INDUSTRY) -> bool:
    """Poll until ready or ``timeout_min`` elapses. Used by the CLI/harness path."""
    deadline = time.monotonic() + timeout_min * 60
    while True:
        ready, reason = check(industry)
        if ready:
            return True
        if time.monotonic() >= deadline:
            log.error(
                "[readiness] giving up after %s min — last probe: %s", timeout_min, reason
            )
            return False
        log.info("[readiness] retrying in %ss", interval_s)
        time.sleep(interval_s)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verisim source readiness probe (0 = ready, 1 = not ready).",
    )
    parser.add_argument("--wait", action="store_true",
                        help="poll until ready instead of probing once")
    parser.add_argument("--timeout-minutes", type=int, default=READINESS_TIMEOUT_MIN)
    parser.add_argument("--interval-seconds", type=int, default=POKE_INTERVAL_S)
    parser.add_argument("--industry", default=INDUSTRY)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    if args.wait:
        ready = wait_until_ready(args.timeout_minutes, args.interval_seconds, args.industry)
    else:
        ready = check(args.industry)[0]
    print("READY" if ready else "NOT READY")
    return 0 if ready else 1


if __name__ == "__main__":
    sys.exit(main())
