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

Readiness definition
--------------------
Ready == **all** of:

1. ``GET /health``                              → ``details[<industry>] == "healthy"``
   (the API process is up)
2. ``GET /<industry>/status``                   → ``state.is_running`` is true and
                                                   ``state.mode == "realtime"``
   (the generator finished bootstrap *and* its 30-day backfill; ``stopped`` and
   ``backfill`` are both "not ready")
3. ``GET /<industry>/stats/backfill-progress``  → ``in_progress`` is false
   (belt-and-braces with (2): the generator also reports targeted gap backfills here)
4. the critical source endpoints (see ``CRITICAL_PROBES``) → ``total > 0``
   (proves the API is serving event rows, not just answering 200s from reference
   data — this is the check that catches "raw table never gets created")

Readiness is *state*-based, not *volume*-based: it never waits for a stable row count,
because in steady state the generator writes continuously and a "no growth for N
seconds" rule would deadlock every scheduled run. The data probes ask for one row
(``limit=1``), so they stay cheap on every poke.

Failure-mode rules (deliberate, see ``evaluate_readiness``)
-----------------------------------------------------------
* Empty source rows or an unreachable data endpoint → **not ready** (the ingest would
  fail on it).
* A **4xx** from a data probe → **not a block**. A 4xx means *this module asked
  wrongly* (e.g. a required query param the endpoint demands), which is a defect here,
  not evidence about the source. It is logged loudly and the generator-state checks
  (1–3) still gate. Without this rule a probe bug would silently deadlock the pipeline
  forever — the same class of bug this module exists to fix.
  (``/pos/transactions`` requires ``start_dt``/``end_dt``; a bare ``limit`` earns a
  HTTP 422 on the live API — verified 2026-09-21.)

Operational notes
-----------------
* Steady state costs one poke (~200 ms): mode is already ``realtime`` and both critical
  endpoints are populated.
* ``mode="reschedule"`` keeps the worker slot free while waiting; the sensor is only
  ever queued, so it cannot starve the other DAGs.
* ``READINESS_TIMEOUT_MIN`` bounds the wait. If the source never becomes ready the run
  fails *loudly, in the sensor*, with the last probe reason in the task log — rather
  than failing later inside dbt with an opaque "relation does not exist". The next
  scheduled interval retries by itself.
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
# fallback (grocery_ingest_api.INCREMENTAL_FALLBACK_DAYS = 365) so the probe looks at
# the same span the first ingest pass will read.
LOOKBACK_DAYS = int(os.getenv("VERISIM_READINESS_LOOKBACK_DAYS", "365"))

# The late-arriving event tables whose emptiness broke the first run. Consumers are
# incremental and have no reference-data seed, so they are empty until the backfill
# produces transactions — and their raw tables are created lazily from the first
# non-empty page, which is exactly how the first run breaks.
#
# (path, accepts_bare_limit): most endpoints answer `?limit=1`; the POS transactions
# endpoint validates the date window and 422s without it, so the probe sends the
# rolling window instead. Keep this in sync with TABLE_CONFIGS in
# grocery_ingest_api.py if a third table ever joins the "empty on fresh install" set.
CRITICAL_PROBES = (
    ("/pos/transactions", False),
    ("/pos/loyalty-point-transactions", True),
)


# ---------------------------------------------------------------------------
# Probe (I/O)
# ---------------------------------------------------------------------------

def _get(path: str, params: Mapping[str, Any] | None = None,
         timeout: float | None = None) -> tuple[int | None, dict | None, str | None]:
    """GET ``path`` → ``(status_code, json_body, error)``.

    ``status_code`` is None for a transport-level failure; ``json_body`` is None when
    the response was not a JSON object.
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
    if not isinstance(payload, dict):
        payload = None
    return resp.status_code, payload, None


def _json(path: str, params: Mapping[str, Any] | None = None,
          timeout: float | None = None) -> dict | None:
    """``_get`` for probes where any failure simply means "cannot prove readiness"."""
    status, payload, error = _get(path, params, timeout)
    if status != 200 or payload is None:
        log.warning("[readiness] %s%s failed (status=%s%s)",
                    API_BASE, path, status, f", {error}" if error else "")
        return None
    return payload


def critical_params(path: str, accepts_bare_limit: bool,
                    now: datetime | None = None) -> dict:
    """Query params for a critical-endpoint probe."""
    params: dict[str, Any] = {"limit": 1}
    if not accepts_bare_limit:
        end = now or datetime.now(timezone.utc)
        params["start_dt"] = (end - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
        params["end_dt"] = end.date().isoformat()
    return params


def collect_evidence(industry: str = INDUSTRY, timeout: float | None = None) -> dict:
    """Gather one readiness snapshot. Pure I/O; the verdict is in evaluate_readiness."""
    critical: dict[str, dict] = {}
    for suffix, accepts_bare_limit in CRITICAL_PROBES:
        path = f"/{industry}{suffix}"
        params = critical_params(path, accepts_bare_limit)
        status, payload, error = _get(path, params, timeout)
        total = payload.get("total") if payload else None
        critical[path] = {
            "params": params,
            "status": status,
            "total": total if isinstance(total, int) else None,
        }
        if status != 200:
            log.warning("[readiness] %s%s -> status=%s%s", API_BASE, path, status,
                        f" ({error})" if error else "")

    return {
        "health": _json("/health", timeout=timeout),
        "status": _json(f"/{industry}/status", timeout=timeout),
        "backfill": _json(f"/{industry}/stats/backfill-progress", timeout=timeout),
        "critical": critical,
    }


# ---------------------------------------------------------------------------
# Verdict (pure — no I/O, unit-testable)
# ---------------------------------------------------------------------------

def evaluate_readiness(evidence: Mapping[str, Any],
                       industry: str = INDUSTRY) -> tuple[bool, str]:
    """Decide readiness from an evidence dict, returning ``(ready, reason)``.

    ``reason`` always explains the verdict and carries the evidence behind it, so the
    sensor's task log is self-diagnosing (which check failed, and with what).
    """
    health = evidence.get("health")
    if health is None:
        return False, "verisim API /health unreachable or non-200"
    if health.get("status") != "healthy":
        return False, f"verisim API /health status={health.get('status')!r}"
    details = health.get("details") or {}
    if details.get(industry) != "healthy":
        return False, f"verisim API reports {industry}={details.get(industry)!r}"

    status = evidence.get("status")
    if status is None:
        return False, f"/{industry}/status unreachable or non-200"
    state = status.get("state") or {}
    if not state:
        return False, f"/{industry}/status returned no generator state"

    mode = state.get("mode")
    if not state.get("is_running"):
        return False, (
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
        return False, f"generator mode={mode!r} — verisim still bootstrapping{extra}"

    backfill = evidence.get("backfill")
    if backfill is None:
        return False, f"/{industry}/stats/backfill-progress unreachable or non-200"
    if backfill.get("in_progress"):
        return False, (
            "backfill in progress "
            f"(pct_complete={backfill.get('pct_complete')}%, "
            f"days_remaining={backfill.get('days_remaining')})"
        )

    empty: list[str] = []
    unreachable: list[str] = []
    rejected: list[str] = []
    populated: list[str] = []
    for path, probe in sorted((evidence.get("critical") or {}).items()):
        probe_status = probe.get("status")
        total = probe.get("total")
        if probe_status is not None and 400 <= probe_status < 500:
            rejected.append(f"{path} (HTTP {probe_status})")
        elif total is None:
            unreachable.append(f"{path} (status={probe_status!r})")
        elif total <= 0:
            empty.append(f"{path} (total={total})")
        else:
            populated.append(f"{path.rsplit('/', 1)[-1]}={total}")

    # A 4xx is this module's own request being wrong, not the source being unready.
    # Never block on it — the generator-state checks above already gate — but say so
    # in both the task log and the verdict reason.
    rejected_note = ""
    if rejected:
        detail = ", ".join(rejected)
        rejected_note = ("; probe(s) rejected by the API, not treated as unreadiness "
                         f"(probe params need fixing): {detail}")
        log.warning("[readiness] probe(s) rejected by the API, not treated as "
                    "unreadiness (probe params need fixing): %s", detail)

    if empty:
        return False, "source endpoint(s) serving no rows yet: " + ", ".join(empty)
    if unreachable:
        return False, "source endpoint(s) unreachable: " + ", ".join(unreachable)

    if not populated:
        return True, f"mode=realtime, backfill complete (no data probes configured){rejected_note}"
    return True, (
        "mode=realtime, backfill complete, critical tables populated "
        f"({', '.join(populated)}){rejected_note}"
    )


# ---------------------------------------------------------------------------
# Sensor / CLI entry points
# ---------------------------------------------------------------------------

def check(industry: str = INDUSTRY, timeout: float | None = None) -> tuple[bool, str]:
    """One probe + verdict, logged."""
    ready, reason = evaluate_readiness(collect_evidence(industry, timeout=timeout), industry)
    log.info("[readiness] %s — %s", "READY" if ready else "NOT READY", reason)
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
