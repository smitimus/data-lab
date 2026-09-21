#!/usr/bin/env python3
"""
Readiness-gate tests for verisim_readiness.py
=============================================
Run INSIDE the airflow-worker container (needs airflow + requests):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_verisim_readiness.py

No pytest required — plain asserts, exit code 0 = pass.

``evaluate_readiness`` is pure (evidence in, verdict out) by design, so every branch
that gates the pipeline is exercised here against a fabricated snapshot instead of
against the live source. The final case does probe the live source and only asserts
that the probe returns a verdict — the *value* of that verdict depends on the host.

Everything runs from ``main()`` under an ``if __name__ == "__main__"`` guard: the
Airflow DagBag imports every .py file under the dags folder, so module-level test
code would execute (and print) on every DAG parse.

Cases:
  1. healthy snapshot                              -> ready
  2. /health unreachable                           -> not ready
  3. /health reports the industry unhealthy        -> not ready
  4. generator stopped (is_running false)          -> not ready
  5. generator backfilling (the bootstrap race)    -> not ready, reason carries pct
  6. mode realtime but targeted backfill running   -> not ready
  7. backfill-progress probe unreachable           -> not ready
  8. a critical source table empty; this is the    -> not ready
     original first-run failure
  9. a critical endpoint unreachable               -> not ready
 10. a probe rejected with HTTP 422                -> still ready, and reported
     (guard against the deadlock class: a probe bug must never block the pipeline)
 11. a critical probe returning HTTP 500           -> not ready
 12. critical_params shape per endpoint           -> window vs bare limit
 13. live probe returns a verdict                  -> informational
"""
from __future__ import annotations

import importlib.util
import io
import logging
import sys

MODULE_PATH = "/opt/airflow/dags/verisim_readiness.py"
spec = importlib.util.spec_from_file_location("verisim_readiness", MODULE_PATH)
vr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vr)

INDUSTRY = vr.INDUSTRY
TXN = f"/{INDUSTRY}/pos/transactions"
LOYALTY = f"/{INDUSTRY}/pos/loyalty-point-transactions"


def probe(status: int | None, total: int | None) -> dict:
    return {"params": {"limit": 1}, "status": status, "total": total}


def healthy_evidence() -> dict:
    """A snapshot of a fully-ready source."""
    return {
        "health": {"status": "healthy", "details": {INDUSTRY: "healthy"}},
        "status": {
            "state": {"is_running": True, "mode": "realtime"},
            "today": {"pos_today": 1234},
        },
        "backfill": {"in_progress": False},
        "critical": {TXN: probe(200, 1_123_689), LOYALTY: probe(200, 453_496)},
    }


class Runner:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def expect(self, expected: bool, evidence: dict, label: str,
               want_in_reason: str | None = None) -> None:
        ready, reason = vr.evaluate_readiness(evidence, INDUSTRY)
        if ready is not expected:
            self.failures.append(f"{label}: expected ready={expected}, got {ready} ({reason})")
            print(f"FAIL  {label}: expected ready={expected}, got {ready} — {reason}")
            return
        if want_in_reason and want_in_reason not in reason:
            self.failures.append(f"{label}: reason missing {want_in_reason!r} — got {reason!r}")
            print(f"FAIL  {label}: reason missing {want_in_reason!r} — {reason!r}")
            return
        print(f"PASS  {label} — {reason}")

    def fail(self, label: str, detail: str) -> None:
        self.failures.append(f"{label}: {detail}")
        print(f"FAIL  {label}: {detail}")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    r = Runner()

    # 1. baseline: everything healthy
    r.expect(True, healthy_evidence(), "healthy snapshot is ready")

    # 2. API down
    ev = healthy_evidence()
    ev["health"] = None
    r.expect(False, ev, "/health unreachable", "unreachable")

    # 3. API up, industry not healthy yet (mid-bootstrap)
    ev = healthy_evidence()
    ev["health"] = {"status": "healthy", "details": {INDUSTRY: "unhealthy"}}
    r.expect(False, ev, "industry unhealthy", "unhealthy")

    # 4. generator stopped
    ev = healthy_evidence()
    ev["status"]["state"] = {"is_running": False, "mode": "stopped"}
    r.expect(False, ev, "generator stopped", "not running")

    # 5. the original race: fresh host, 30-day backfill in flight
    ev = healthy_evidence()
    ev["status"]["state"] = {"is_running": True, "mode": "backfill"}
    ev["backfill"] = {"in_progress": True, "pct_complete": 12.5, "days_remaining": 27}
    r.expect(False, ev, "generator backfilling", "still bootstrapping")
    r.expect(False, ev, "backfill reason carries progress", "pct_complete=12.5")

    # 6. /status lost the race with a targeted gap backfill
    ev = healthy_evidence()
    ev["backfill"] = {"in_progress": True, "pct_complete": 80.0, "days_remaining": 1}
    r.expect(False, ev, "targeted backfill in progress", "backfill in progress")

    # 7. backfill progress endpoint down — cannot prove readiness, so not ready
    ev = healthy_evidence()
    ev["backfill"] = None
    r.expect(False, ev, "backfill-progress unreachable", "unreachable")

    # 8. the documented first-run failure: raw table never created because the
    #    endpoint served no rows
    ev = healthy_evidence()
    ev["critical"][LOYALTY] = probe(200, 0)
    r.expect(False, ev, "critical table empty", "loyalty-point-transactions")
    r.expect(False, ev, "empty-table reason carries the total", "total=0")

    # 9. critical endpoint unreachable
    ev = healthy_evidence()
    ev["critical"][TXN] = probe(None, None)
    r.expect(False, ev, "critical endpoint unreachable", "unreachable")

    # 10. probe rejected (HTTP 422 — e.g. a required query param this module forgot).
    #     That is our bug, not the source's: it must NOT deadlock the gate.
    ev = healthy_evidence()
    ev["critical"][TXN] = probe(422, None)
    captured = io.StringIO()
    handler = logging.StreamHandler(captured)
    old_level = vr.log.level
    vr.log.addHandler(handler)
    vr.log.setLevel(logging.WARNING)
    try:
        ready, reason = vr.evaluate_readiness(ev, INDUSTRY)
        captured_text = captured.getvalue()
    finally:
        vr.log.removeHandler(handler)
        vr.log.setLevel(old_level)
    if not ready:
        r.fail("probe 422 must not block", f"got ready={ready} ({reason})")
    elif "HTTP 422" not in reason:
        r.fail("probe 422 must be reported in the verdict reason", f"reason={reason!r}")
    elif "probe params need fixing" not in captured_text:
        r.fail("probe 422 must be logged loudly", f"log={captured_text!r}")
    else:
        print(f"PASS  probe 422 does not block, and is reported — {reason}")

    # 11. a 5xx on a data probe is a real failure signal
    ev = healthy_evidence()
    ev["critical"][TXN] = probe(500, None)
    r.expect(False, ev, "critical probe 5xx", "unreachable")

    # 12. probe params: the POS transactions endpoint validates the date window
    params = vr.critical_params(TXN, False)
    if not {"limit", "start_dt", "end_dt"} <= set(params):
        r.fail("critical_params window", f"missing date window: {params}")
    elif set(vr.critical_params(LOYALTY, True)) != {"limit"}:
        r.fail("critical_params bare", f"unexpected params: {vr.critical_params(LOYALTY, True)}")
    else:
        print(f"PASS  critical_params: window={params} "
              f"bare={vr.critical_params(LOYALTY, True)}")

    # 13. live probe: must return a verdict without raising. Host-specific value.
    live_ready, live_reason = vr.check()
    if not isinstance(live_ready, bool):
        r.fail("live probe", "check() must return a bool")
    else:
        print(f"INFO  live probe on this host: ready={live_ready} — {live_reason}")

    print()
    if r.failures:
        print(f"FAILED ({len(r.failures)}):")
        for f in r.failures:
            print(f"  - {f}")
        return 1
    print("All readiness-gate cases passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
