#!/usr/bin/env python3
"""
Readiness-gate tests for verisim_readiness.py
=============================================
Run INSIDE the airflow-worker container (needs airflow + requests + the dags dir):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_verisim_readiness.py

No pytest required — plain asserts, exit code 0 = pass.

``evaluate_readiness`` is pure (evidence in, verdict out) by design, so every branch
that gates the pipeline is exercised here against a fabricated snapshot instead of
against the live source. The final case does probe the live source and only asserts
that the probe returns a verdict — the *value* of that verdict depends on the host.

Everything runs from ``main()`` under an ``if __name__ == "__main__"`` guard: the
Airflow DagBag imports every .py file under the dags folder (this directory is not
excluded by .airflowignore), so module-level test code would execute on every parse.

Cases:
  1. healthy snapshot                              -> ready
  2. /health unreachable                           -> not ready
  3. /health reports the industry unhealthy        -> not ready
  4. generator stopped (is_running false)          -> not ready
  5. generator backfilling (the bootstrap race)    -> not ready, reason carries pct
  6. mode realtime but targeted backfill running   -> not ready
  7. backfill-progress probe unreachable           -> not ready
  8. the probe list is DERIVED from the ingest DAG  -> one probe per TABLE_CONFIGS
     registry, names the source relation, and      (the t_17927141 anti-regression:
     mirrors the load's request shape               the old hand-maintained pair of
                                                    "critical" endpoints could not
                                                    fail this test)
  9. the t_17927141 failure itself: endpoints the  -> not ready, and the reason names
     source does not serve (HTTP 404)                  every missing SOURCE RELATION
 10. a required endpoint empty with no raw table   -> not ready (the original
     yet (the documented first-run race)               first-run failure, now for any
                                                       table instead of two)
 11. the same endpoint empty but already loaded    -> still ready, reported
 12. EDW raw-table inventory unreadable + empty    -> still ready (the gate must not
     endpoint                                          block on its own blindness)
 13. a probe rejected with HTTP 422 (not 404)      -> still ready, and reported
     (guard against the deadlock class: a probe bug must never block the pipeline)
 14. a required probe returning HTTP 500           -> not ready
 15. the ingest registry cannot be imported        -> not ready, says which module
 16. a ready generator with no probes at all       -> not ready (no evidence, no pass)
 17. a bootstrapping generator skips the 32 probes -> 3 requests, not 35
 18. live probe returns a verdict                  -> informational
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
ONLINE_ORDERS = f"/{INDUSTRY}/online/orders"
POS_RETURNS = f"/{INDUSTRY}/pos/returns"

# The five relations the dev source image did not serve (t_17927141).
MISSING_ON_DEV = [
    (f"/{INDUSTRY}/pos/returns", "pos.returns"),
    (f"/{INDUSTRY}/pos/return-items", "pos.return_items"),
    (f"/{INDUSTRY}/online/orders", "online.orders"),
    (f"/{INDUSTRY}/online/order-items", "online.order_items"),
    (f"/{INDUSTRY}/online/order-events", "online.order_events"),
]


def probe(status: int | None, total: int | None, relation: str = "x.y",
          raw_table: str = "raw_x.y", task_id: str = "t") -> dict:
    return {"params": {"limit": 1}, "status": status, "total": total,
            "relation": relation, "raw_table": raw_table, "task_id": task_id}


def probe_map() -> dict:
    """One populated probe per source relation the ingest DAG loads."""
    return {
        path: probe(200, 1000, relation, raw_table, task_id)
        for path, _params, relation, raw_table, task_id in vr.required_probes()
    }


def healthy_evidence() -> dict:
    """A snapshot of a fully-ready source, covering every ingested relation."""
    return {
        "health": {"status": "healthy", "details": {INDUSTRY: "healthy"}},
        "status": {
            "state": {"is_running": True, "mode": "realtime"},
            "today": {"pos_today": 1234},
        },
        "backfill": {"in_progress": False},
        "probes": probe_map(),
        "probe_error": None,
        "probes_skipped": None,
        # every raw table exists (an empty page is then a fact about the source)
        "raw_tables": {p["raw_table"] for p in probe_map().values()},
        "raw_tables_error": None,
    }


class Runner:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def expect(self, expected: bool, evidence: dict, label: str,
               want_in_reason: str | list[str] | None = None) -> None:
        ready, reason = vr.evaluate_readiness(evidence, INDUSTRY)
        if ready is not expected:
            self.failures.append(f"{label}: expected ready={expected}, got {ready} ({reason})")
            print(f"FAIL  {label}: expected ready={expected}, got {ready} — {reason}")
            return
        wanted = [want_in_reason] if isinstance(want_in_reason, str) else (want_in_reason or [])
        for want in wanted:
            if want not in reason:
                self.failures.append(f"{label}: reason missing {want!r} — got {reason!r}")
                print(f"FAIL  {label}: reason missing {want!r} — {reason!r}")
                return
        print(f"PASS  {label} — {reason}")

    def fail(self, label: str, detail: str) -> None:
        self.failures.append(f"{label}: {detail}")
        print(f"FAIL  {label}: {detail}")

    def note(self, label: str, detail: str) -> None:
        print(f"PASS  {label} — {detail}")


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
    ev["probes"] = {}
    ev["probes_skipped"] = "generator mode='backfill'"
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

    # 8. the probe list is derived from the ingest DAG's own registry (t_17927141).
    #    The old module carried two hand-picked "critical" endpoints, so this is the
    #    test that would have caught the drift that let the dev run through the gate.
    configs, relations = vr._ingest_registry()
    probes = vr.required_probes()
    probe_paths = [p[0] for p in probes]
    expected_paths = [c[1] for c in configs]
    problems = []
    if len(probes) != len(configs):
        problems.append(f"{len(probes)} probes for {len(configs)} ingested tables")
    if sorted(probe_paths) != sorted(expected_paths):
        problems.append("probe paths do not match the ingest's endpoint list")
    if len(set(probe_paths)) != len(probe_paths):
        problems.append("duplicate probe paths")
    for path, params, relation, raw_table, task_id in probes:
        if relation != relations[task_id]:
            problems.append(f"{task_id}: relation {relation!r} != SOURCE_RELATIONS entry")
        if not relation or "." not in relation:
            problems.append(f"{task_id}: relation {relation!r} is not schema-qualified")
    # request shape must mirror the load: window params exactly where the row declares
    # them, bare limit elsewhere
    for (task_id, api_path, _s, _t, _pk, _strat, _wm, start_param, end_param), entry in zip(
            configs, probes):
        _path, params, _rel, _raw, _tid = entry
        if start_param:
            if start_param not in params or end_param not in params or "limit" not in params:
                problems.append(f"{task_id}: window probe missing params ({sorted(params)})")
        elif set(params) != {"limit"}:
            problems.append(f"{task_id}: undated probe should send a bare limit, sent {sorted(params)}")
    if problems:
        r.fail("probe list derived from the ingest registry", "; ".join(problems))
    else:
        r.note("probe list derived from the ingest registry",
               f"{len(probes)} probes, one per ingested table "
               f"({len([p for p in probes if 'start_dt' in p[1]])} with a date window)")

    # 9. the t_17927141 failure: the source does not serve endpoints the DAG ingests.
    #    Every one of them must be named, by source relation, in the verdict.
    ev = healthy_evidence()
    for api_path, relation in MISSING_ON_DEV:
        ev["probes"][api_path] = probe(404, None, relation, f"raw_{relation}", relation)
    ready, reason = vr.evaluate_readiness(ev, INDUSTRY)
    if ready:
        r.fail("missing source relations block", f"gate passed with {reason}")
    else:
        absent = [rel for _p, rel in MISSING_ON_DEV if rel not in reason]
        if absent:
            r.fail("missing source relations are named",
                   f"reason does not name {absent} — {reason}")
        elif "HTTP 404" not in reason:
            r.fail("missing source relation reason carries the status", reason)
        else:
            print(f"PASS  missing source relations block the gate and are named — {reason}")
    r.expect(False, ev, "one missing relation is enough", "online.orders")

    # 10. the documented first-run race, now for any table: the endpoint answers 200
    #     with no rows and its raw table does not exist yet, so the ingest would create
    #     nothing and dbt staging would fail on a missing relation.
    ev = healthy_evidence()
    ev["probes"][LOYALTY] = probe(200, 0, "pos.loyalty_point_transactions",
                                  "raw_pos.loyalty_point_transactions",
                                  "pos_loyalty_point_transactions")
    ev["raw_tables"].discard("raw_pos.loyalty_point_transactions")
    r.expect(False, ev, "empty source relation with no raw table blocks",
             ["loyalty-point-transactions", "total=0", "never be created"])

    # 11. the same empty endpoint once its raw table exists: a fact about the source
    #     that waiting cannot change — reported, not a reason to stall the pipeline.
    ev = healthy_evidence()
    ev["probes"][LOYALTY] = probe(200, 0, "pos.loyalty_point_transactions",
                                  "raw_pos.loyalty_point_transactions",
                                  "pos_loyalty_point_transactions")
    ready, reason = vr.evaluate_readiness(ev, INDUSTRY)
    if not ready:
        r.fail("empty but already-loaded relation must not block", reason)
    elif "loyalty-point-transactions" not in reason:
        r.fail("empty-but-loaded relation is reported in the reason", reason)
    else:
        print(f"PASS  empty but already-loaded relation does not block, and is reported — {reason}")

    # 12. EDW inventory unreadable: the gate cannot tell "not loaded yet" from "loaded",
    #     and its own blindness must not be what halts the pipeline.
    ev = healthy_evidence()
    ev["probes"][LOYALTY] = probe(200, 0, "pos.loyalty_point_transactions",
                                  "raw_pos.loyalty_point_transactions",
                                  "pos_loyalty_point_transactions")
    ev["raw_tables"] = None
    ev["raw_tables_error"] = "OperationalError: could not connect to server"
    r.expect(True, ev, "unreadable EDW inventory does not block on emptiness")

    # 13. probe rejected (HTTP 422 — e.g. a required query param this module forgot).
    #     That is our bug, not the source's: it must NOT deadlock the gate.
    ev = healthy_evidence()
    ev["probes"][TXN] = probe(422, None, "pos.transactions", "raw_pos.transactions",
                              "pos_transactions")
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

    # 14. a 5xx on a required probe is a real failure signal
    ev = healthy_evidence()
    ev["probes"][TXN] = probe(500, None, "pos.transactions", "raw_pos.transactions",
                              "pos_transactions")
    r.expect(False, ev, "required probe 5xx", "unreachable")

    # 15. the registry itself cannot be read: refuse to guess, and say why
    ev = healthy_evidence()
    ev["probes"] = {}
    ev["probe_error"] = "ModuleNotFoundError: No module named 'grocery_ingest_api'"
    r.expect(False, ev, "unreadable ingest registry blocks",
             ["cannot verify the source", "grocery_ingest_api"])

    # 16. a ready generator with nothing to check must not pass either
    ev = healthy_evidence()
    ev["probes"] = {}
    r.expect(False, ev, "no probes is not a pass", "refusing to declare readiness")

    # 17. a bootstrapping generator short-circuits the 32 data probes: the state checks
    #     cost three requests, and the source is not probed 32 times a minute while it
    #     builds its history.
    state_paths = {"/health", f"/{INDUSTRY}/status", f"/{INDUSTRY}/stats/backfill-progress"}
    calls: list[str] = []
    orig_json, orig_get = vr._json, vr._get

    def fake_json(path, params=None, timeout=None):
        calls.append(path)
        if path == "/health":
            return {"status": "healthy", "details": {INDUSTRY: "healthy"}}
        if path.endswith("/status"):
            return {"state": {"is_running": True, "mode": "backfill"}}
        return {"in_progress": True, "pct_complete": 3.0, "days_remaining": 29}

    def fake_get(path, params=None, timeout=None):
        calls.append(path)
        return 200, {"data": [{"x": 1}], "total": 5}, None

    vr._json, vr._get = fake_json, fake_get
    try:
        ev = vr.collect_evidence(INDUSTRY)
    finally:
        vr._json, vr._get = orig_json, orig_get
    data_calls = [c for c in calls if c not in state_paths]
    if data_calls:
        r.fail("bootstrapping short-circuits the data probes",
               f"still probed {len(data_calls)} endpoint(s): {data_calls[:3]}")
    elif not ev.get("probes_skipped"):
        r.fail("bootstrapping short-circuit is recorded", "probes_skipped is empty")
    elif len(calls) != len(state_paths):
        r.fail("bootstrapping costs three requests", f"made {len(calls)}: {calls}")
    else:
        r.note("bootstrapping generator costs 3 requests, not 35",
               f"probes_skipped={ev['probes_skipped']!r}")

    # 18. live probe: must return a verdict without raising. Host-specific value.
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
