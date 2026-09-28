#!/usr/bin/env python3
"""
Load-order tests (t_7e427ee6)
=============================
Run INSIDE the airflow-worker container (needs airflow, dbt's yaml, and reach to
the DAG directory):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_load_order.py

No pytest required — plain asserts, exit code 0 = pass.

What it proves, and why each part is load-bearing:

  1. Coverage. Every cross-route `relationships` test in the dbt staging suite
     has a matching entry in `REFERENCING_ROUTES` (child route first), and
     `REFERENCING_ROUTES` declares nothing the dbt suite does not assert. That
     suite is the source of truth here: adding an FK test without declaring its
     read order fails this test, which is the point — the FK is only assertable
     because the read order makes it true.
  2. Structure. Every declared pair is actually wired in the DAG: a path exists
     from the child route to the parent route, and none from the parent to the
     child, so the referenced route can never read first. Checked transitively,
     which is what a chain such as
     `pos_return_items → pos_transaction_items → pos_transactions` relies on.
  3. The incident. `online_order_events` and `online_order_items` are read
     before `online_orders`. That pair is the reason this test exists: reading
     the events route after the orders route left 10 events at 20:04:13.717 with
     no parent order on the test slot's fresh seed (t_7e427ee6), 8 on the dev
     slot. `online_order_events` is incremental since t_5a16129f (it used to be a
     FULL reload with no window), which is what makes its read cut an instant
     both windows can end at rather than its own last read — see 5d/5e.
  4. The read cut. Every referenced route takes the read end of its referencing
     routes as the end of its own window (`cut_from`), and every provider it
     names is upstream of it. This is the other half of the ordering: reading the
     child first fixes the missing parent, but a window that then runs to the
     parent's own task start would load a completed order whose final event the
     child had not read yet — the same defect mirrored, against
     `assert_online_orders_reconcile`. Both sides are judged at one instant now.
  5. The as-of pair (t_5a16129f). `online_orders`' rows MUTATE after insert, so an
     insert clock cannot bound the state it loads: it is declared in
     `AS_OF_ROUTES` with the clock that moves with the mutation (`updated_at`) and
     is bounded by the read instant of the route that shares that clock
     (`online_order_events`), not by its own task start and not by the latest of
     its children's cuts. The declaration, the `TABLE_CONFIGS` entry, the read
     order and the DAG wiring have to agree — that is what 5a–5f assert.
  6. The cluster's single instant (t_5a16129f). An as-of pair is one cluster, and
     every route in it ends its window at the same instant: `online_order_items`
     is `SNAPSHOT_BOUND_ROUTES`-bound to `online_order_events` too, because the
     order it references is inserted by the same transaction as the item. Left at
     its own task start it would load a row the state-bounded orders window does
     not carry — the orphan class t_7e427ee6 closed, on the insert-clock side.
     6a–6d assert the declaration, the cut, the provider being windowed and the
     one instant being shared with the as-of route.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from collections import deque

import yaml

DAGS_DIR = os.environ.get("DAGS_DIR", "/opt/airflow/dags")
STAGING_YML = os.environ.get(
    "STAGING_YML",
    "/opt/airflow/dbt/grocery/models/staging/staging.yml",
)

# Staging models whose name is not `stg_<route>`: the two HR masters predate the
# `hr_` route prefix. `stg_hr_schedules` does shorten to its route.
MODEL_ROUTE_ALIASES = {
    "stg_employees": "hr_employees",
    "stg_locations": "hr_locations",
}

PASS = []
FAIL = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS " if cond else "FAIL ") + name + (f" — {detail}" if detail else ""))


def load(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def ref_name(to):
    """`ref('stg_locations')` → `stg_locations`; anything else returned as-is."""
    if not isinstance(to, str):
        return to
    if "'" in to:
        return to.split("'")[1]
    return to.strip()


def route_of(model_name):
    """staging model name → ingest route (task_id).

    Usually 1:1 (`stg_online_orders` → `online_orders`), with two exceptions where
    the staging model kept a shorter name than the route that serves it.
    """
    if model_name in MODEL_ROUTE_ALIASES:
        return MODEL_ROUTE_ALIASES[model_name]
    return model_name[4:] if model_name.startswith("stg_") else None


def fk_edges_from_suite():
    """Cross-route FK edges the dbt staging suite asserts, as (child, parent) models."""
    doc = yaml.safe_load(open(STAGING_YML))
    edges, unmapped = set(), set()
    for model in doc.get("models") or []:
        child = model.get("name")
        for col in model.get("columns") or []:
            tests = col.get("tests") or col.get("data_tests") or []
            for test in tests:
                if not isinstance(test, dict) or "relationships" not in test:
                    continue
                parent = ref_name(test["relationships"].get("to"))
                if route_of(child) is None or route_of(parent) is None:
                    unmapped.add(f"{child}.{col.get('name')} -> {parent}")
                    continue
                edges.add((route_of(child), route_of(parent)))
    return edges, unmapped


def reachable(downstream, start, goal=None, seen=None, path=None):
    """BFS over task ids. Returns the path to `goal`, or all reachable ids."""
    seen = set() if seen is None else seen
    queue = deque([(start, [start])])
    found = []
    while queue:
        node, walk = queue.popleft()
        if node in seen:
            continue
        seen.add(node)
        if goal is not None and node == goal:
            return walk
        if goal is None:
            found.append(node)
        for nxt in sorted(downstream.get(node, ())):
            if nxt not in seen:
                queue.append((nxt, walk + [nxt]))
    return found if goal is None else None


def main():
    gia = load(f"{DAGS_DIR}/grocery_ingest_api.py", "gia_load_order")

    declared = list(gia.REFERENCING_ROUTES)
    declared_set = set(declared)
    suite_edges, unmapped = fk_edges_from_suite()
    route_ids = {c[0] for c in gia.TABLE_CONFIGS}

    # ------------------------------------------------------------------
    # 1. coverage — the dbt suite and the declaration agree, both ways
    # ------------------------------------------------------------------
    check("1a every dbt FK edge is mapped onto two ingest routes",
          not unmapped, str(sorted(unmapped))[:300])
    missing = sorted(suite_edges - declared_set)
    check("1b every dbt FK edge has a declared read order",
          not missing, str(missing)[:400])
    extra = sorted(declared_set - suite_edges)
    check("1c REFERENCING_ROUTES declares no edge the dbt suite does not assert",
          not extra, str(extra)[:400])
    check("1d no duplicate pairs declared", len(declared) == len(declared_set),
          f"{len(declared)} declared, {len(declared_set)} distinct")
    check("1e every declared route is a configured ingest route",
          all(r in route_ids for pair in declared for r in pair),
          str(sorted({r for pair in declared for r in pair} - route_ids)))
    check("1f no route references itself",
          all(child != parent for child, parent in declared))

    # ------------------------------------------------------------------
    # 2. structure — the DAG wires each pair child → parent, and only that way
    # ------------------------------------------------------------------
    # TaskGroup namespacing prefixes task ids (`ingest_pos.pos_transactions`), so
    # normalise to the route name — the same key REFERENCING_ROUTES uses.
    downstream = {}
    for task in gia.dag.tasks:
        key = task.task_id.rsplit(".", 1)[-1]
        downstream[key] = {t.rsplit(".", 1)[-1] for t in task.downstream_task_ids}
    route_tasks = sorted(t for t in downstream if t in route_ids)
    check("2a every configured route is a DAG task",
          len(route_tasks) == len(route_ids),
          f"{len(route_tasks)}/{len(route_ids)}")

    unwired, backwards = [], []
    for child, parent in declared:
        if reachable(downstream, child, goal=parent) is None:
            unwired.append(f"{child} -/-> {parent}")
        if reachable(downstream, parent, goal=child) is not None:
            backwards.append(f"{parent} -> {child}")
    check("2b every declared pair is wired child-before-parent",
          not unwired, str(unwired)[:400])
    check("2c no declared pair is wired parent-before-child",
          not backwards, str(backwards)[:400])

    # A cycle anywhere in the FK graph would make 2b unsatisfiable for some edge;
    # assert it directly so the failure names the node rather than a missing path.
    # (A node is on a cycle when one of its own successors can reach it back.)
    cyclic = sorted(
        t for t in route_tasks
        if any(reachable(downstream, nxt, goal=t) is not None
               for nxt in downstream.get(t, ()))
    )
    check("2d the read-order graph is acyclic", not cyclic, str(cyclic)[:300])

    # ------------------------------------------------------------------
    # 3. the incident this card is about (t_7e427ee6)
    # ------------------------------------------------------------------
    for child in ("online_order_events", "online_order_items"):
        path = reachable(downstream, child, goal="online_orders")
        check(f"3 {child} is read before online_orders",
              path is not None, str(path) if path else "no path")

    # ------------------------------------------------------------------
    # 4. the read cut — a referenced route takes its children's read end as the
    #    end of its window, so both sides of a foreign key are judged at one
    #    instant instead of across the scheduler's gap between the two tasks
    # ------------------------------------------------------------------
    op_by_route = {}
    for task in gia.dag.tasks:
        key = task.task_id.rsplit(".", 1)[-1]
        if key in route_ids:
            op_by_route[key] = task
    cut_from = {}
    for child, parent in declared:
        cut_from.setdefault(parent, set()).add(child)
    cut_wiring, cut_orphans = [], []
    for parent, children in sorted(cut_from.items()):
        op = op_by_route.get(parent)
        if op is None:
            continue
        got = list((op.op_kwargs or {}).get("cut_from") or [])
        if got != sorted(children):
            cut_wiring.append(f"{parent}: {got} != {sorted(children)}")
        # The cut is only there to be used if the providers have already run —
        # which is the same edge the read order asserts, so re-assert it here:
        # a plumbing that names a task that is not upstream would silently fall
        # back to the parent's own task start.
        for child in got:
            if reachable(downstream, child, goal=parent) is None:
                cut_orphans.append(f"{parent} <- {child}")
    check("4a every referenced route takes exactly its children's read cut",
          not cut_wiring, str(cut_wiring)[:400])
    check("4b every cut provider is upstream of the route that uses it",
          not cut_orphans, str(cut_orphans)[:400])
    check("4c a route nothing references and that is not snapshot-bound carries no cut "
          "(own task start, as before)",
          all(not (op.op_kwargs or {}).get("cut_from")
              for r, op in op_by_route.items()
              if r not in cut_from
              and r not in (getattr(gia, "SNAPSHOT_BOUND_ROUTES", {}) or {})),
          str(sorted(r for r, op in op_by_route.items()
                     if r not in cut_from
                     and r not in (getattr(gia, "SNAPSHOT_BOUND_ROUTES", {}) or {})
                     and (op.op_kwargs or {}).get("cut_from")))[:300])

    # ------------------------------------------------------------------
    # 5. the as-of pair (t_5a16129f) — a referenced route whose rows MUTATE
    #    after insert is bounded by the read instant of the route that shares
    #    its clock, not by its own task start
    # ------------------------------------------------------------------
    cfg = {c[0]: c for c in gia.TABLE_CONFIGS}
    as_of = dict(getattr(gia, "AS_OF_ROUTES", {}) or {})

    well_formed = all(
        c.get("state_clock") and c.get("bounds") and c.get("bounded_by")
        for c in as_of.values()
    )
    check("5a every as-of route declares a clock, its bounds and its bounder",
          bool(as_of) and well_formed, str(as_of))

    for route, decl in sorted(as_of.items()):
        c = cfg.get(route)
        clock = decl.get("state_clock")
        bounds = tuple(decl.get("bounds") or ())
        provider = decl.get("bounded_by")

        # The route is anchored on the clock that MOVES with the mutation, and
        # windowed by it — an insert clock here would either miss the state change
        # or (bounded by the child's instant) lose the row instead of delaying it.
        c_shape = (c[5], c[6], c[7], c[8]) if c else None
        check(f"5b {route} is configured on its declared state clock and bounds",
              c_shape == ("incremental", clock, *bounds),
              f"config={c_shape} declared={('incremental', clock, *bounds)}")

        # ... and the instant it is bounded by comes from a route it already
        # declares as a child, read first (REFERENCING_ROUTES × the DAG edges).
        check(f"5c {route}'s bound comes from {provider}, declared as its child",
              (provider, route) in declared_set,
              f"declared pairs including {route}: "
              f"{sorted(p for p in declared if route in p)}")

        # The provider must be WINDOWED. An incremental route's cut is its window
        # end — one instant both windows can end at. A `full` provider returns its
        # last read instead (and ignores the cut itself), so the pair would end at
        # two different instants: the half-wired state t_5a16129f closed.
        p_shape = (cfg[provider][5], cfg[provider][6], cfg[provider][7],
                   cfg[provider][8]) if provider in cfg else None
        check(f"5d {provider} is windowed, so its cut is an instant and not a read",
              p_shape is not None and p_shape[0] == "incremental"
              and p_shape[2] and p_shape[3] and p_shape[3].endswith("_before"),
              f"{provider} config={p_shape}")

        # The cut is taken from that provider *specifically*: a plain cut_from
        # route uses the latest of its children's cuts, which here would let a
        # sibling child that started a second later push the bound past the window
        # the events were read in.
        op = op_by_route.get(route)
        got_cut_from = list((op.op_kwargs or {}).get("cut_from") or []) if op else []
        check(f"5e {route} takes its cut from {provider} specifically",
              provider in got_cut_from,
              f"cut_from={got_cut_from}")

        # The bound is the state clock's own `*_before` bound — the shape the
        # provider's window ends on too — and NOT the insert clock's, so both
        # sides of the pair end on the same kind of instant. Named, not compared:
        # the value is runtime (test_incremental_watermarks 5b drives it).
        p_end = cfg[provider][8] if provider in cfg else None
        check(f"5f {route}'s end bound is its state clock's own end bound, "
              f"not an insert-clock one",
              c is not None and p_end is not None
              and c[8] == f"{clock.rsplit('_', 1)[0]}_before" and c[8] != p_end,
              f"{route}.end={c[8] if c else None} {provider}.end={p_end}")

    # ------------------------------------------------------------------
    # 6. the cluster's single instant (t_5a16129f) — a route that must end its
    #    window at another route's read instant without referencing it, so the
    #    whole cluster is judged at one point in time (state AND foreign key)
    # ------------------------------------------------------------------
    snapshot = dict(getattr(gia, "SNAPSHOT_BOUND_ROUTES", {}) or {})

    check("6a every snapshot-bound route names a declared instant route",
          bool(snapshot) and all(k in cfg and v in cfg and k != v
                                 for k, v in snapshot.items()),
          str(snapshot))

    as_of_providers = {d.get("bounded_by") for d in as_of.values()}
    for route, instant_route in sorted(snapshot.items()):
        # The bound has to reach the task as a cut provider AND the provider has
        # to be read first — a name without the edge would fall back to the
        # route's own task start, which is the instant this declaration exists to
        # stop using.
        op = op_by_route.get(route)
        got = list((op.op_kwargs or {}).get("cut_from") or []) if op else []
        check(f"6b {route} takes the cut of {instant_route} it is bound to",
              instant_route in got, f"cut_from={got}")

        # The provider must be WINDOWED: an incremental route's cut is its window
        # end, one instant another route's window can end at. A `full` route
        # returns its last read instead, which is a different instant every run.
        i_shape = (cfg[instant_route][5], cfg[instant_route][6],
                   cfg[instant_route][7], cfg[instant_route][8])
        check(f"6c {instant_route} is windowed, so {route} ends at an instant",
              i_shape[0] == "incremental" and i_shape[2] and i_shape[3]
              and i_shape[3].endswith("_before"),
              f"{instant_route} config={i_shape}")

        # ... and it is the SAME instant the state route of the cluster is
        # bounded by, which is what makes this one cluster rather than two: the
        # state read and the foreign key are then judged at one point in time.
        check(f"6d {instant_route} is the instant the as-of route is bounded by "
              f"(one instant for the cluster)",
              instant_route in as_of_providers,
              f"as-of providers={sorted(p for p in as_of_providers if p)}")

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed:", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    main()
