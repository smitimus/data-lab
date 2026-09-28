#!/usr/bin/env python3
"""
Staging dependency-order tests for grocery_dbt.py (t_b48af51f)
=============================================================
Run INSIDE the airflow-worker container (needs airflow + the dags dir and the
dbt project on disk):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_dbt_staging_order.py

No pytest required — plain asserts, exit code 0 = pass. Paths are overridable so
the suite can be pointed at a candidate tree (DAGS_DIR, DBT_PROJECT).

What it proves, and why each part is load-bearing:

  The incident. dbt-postgres rebuilds a VIEW by renaming the old one to
  `x__dbt_backup`, creating the new `x`, and then dropping the backup with
  CASCADE. A view that reads `x` does not get re-pointed: it FOLLOWS THE RENAME
  onto the backup by OID, so that CASCADE deletes it. `stg_pos_transaction_items`
  reads `stg_pos_products`, so any run in which `stg_pos_products` is rebuilt
  after the dependent — and nothing rebuilds the dependent afterwards — leaves
  `staging.stg_pos_transaction_items` MISSING: its 14 tests plus the two
  intermediate models that read it fail with `relation ... does not exist`, and
  the retries cannot recover because no task re-creates the view (CT107,
  2026-09-21). Per-model Airflow tasks (`dbt run --select <model>` × 32, in
  parallel) cannot express an order dbt would enforce, so which of the two
  finished second was a coin flip per cycle. The DAG now runs the layer as ONE
  `dbt run --select staging`, which hands the ordering back to dbt.

  So the property that has to hold is: for every staging model that `ref`s
  another staging model, the dependency is built before the dependent *in the
  same dbt process* — or, if the layer is ever split back into per-model tasks,
  the Airflow graph orders the two tasks. This suite asserts exactly that, and
  nothing else: it reads the dbt models and the DAG, no database.

Cases:
  1a every staging→staging `ref` on disk is declared in INTRA_STAGING_REFS
  1b every declared pair still exists on disk (no declaration rot)
  1c no model refs itself
  1d the intra-staging ref graph is acyclic (dbt needs a topological order)
  2a every staging model on disk is built by a `dbt run` task of the DAG
  2b every staging model is built by exactly ONE such task
  3  the hazard's precondition holds: staging is materialized as a VIEW, so the
     rename + drop-cascade swap is what the layer is exposed to
  4a every declared pair is ordered — the same dbt invocation builds both, or
     the DAG has a path dependency-task → dependent-task
  4b the invocation that builds a dependent also builds its dependency (a task
     that rebuilds only the dependent is the shape that produced the incident)
  5  the incident pair is pinned: both models resolve to one build task, and
     that task selects both, so the swap for `stg_pos_products` always precedes
     the rebuild of `stg_pos_transaction_items` in the same run
"""
from __future__ import annotations

import importlib.util
import os
import re
import sys
from collections import deque

import yaml

DAGS_DIR = os.environ.get("DAGS_DIR", "/opt/airflow/dags")
DBT_PROJECT = os.environ.get("DBT_PROJECT", "/opt/airflow/dbt/grocery")
STAGING_DIR = os.path.join(DBT_PROJECT, "models", "staging")
PROJECT_YML = os.path.join(DBT_PROJECT, "dbt_project.yml")
LAYER = "staging"

# Every staging model that reads another staging model, as (dependent, dependency).
# This list is the source of truth for "the author has seen this pair": check 1a
# fails the moment a new intra-staging `ref` appears, so whoever adds one has to
# come here and reason about the build order (that is the whole point — a new
# `ref` between staging models re-opens the t_b48af51f hazard silently otherwise).
# Source of truth for the pair ITSELF is the SQL on disk; check 1b fails if a
# declaration outlives its ref.
INTRA_STAGING_REFS = [
    # stg_pos_transaction_items joins stg_pos_products for department_id. The
    # dependent mirrors onto the dependency's __dbt_backup and dies in its
    # trailing `drop ... cascade` — see the docstring.
    ("stg_pos_transaction_items", "stg_pos_products"),
]

REF_RE = re.compile(r"ref\(\s*['\"]([A-Za-z0-9_]+)['\"]\s*\)")
DBT_RUN_RE = re.compile(r"\bdbt\s+run\b")

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


def staging_models():
    """staging model name → the staging models its SQL refs."""
    refs = {}
    for fn in sorted(os.listdir(STAGING_DIR)):
        if not fn.endswith(".sql"):
            continue
        name = fn[:-4]
        with open(os.path.join(STAGING_DIR, fn)) as fh:
            refs[name] = set(REF_RE.findall(fh.read()))
    return refs


def selectors(bash_command):
    """The `--select` tokens of a dbt command, in order.

    Only what this suite needs: `--select` runs to the next `--flag`. Selector
    syntax (+, tags, paths) is normalised by `matches` below.
    """
    toks = bash_command.split()
    out = []
    for i, tok in enumerate(toks):
        if tok != "--select":
            continue
        for nxt in toks[i + 1:]:
            if nxt.startswith("--"):
                break
            out.append(nxt)
    return out


def matches(selector, name):
    """Does a dbt selector token build the model `name` (a staging model)?"""
    sel = selector.strip("+")
    if sel.startswith("path:"):
        sel = sel[len("path:"):].strip("/")
    return sel in (name, LAYER, f"{LAYER}/", f"models/{LAYER}")


def builds(bash_command):
    """Names of the staging models this dbt run command would build."""
    return {name for name in MODELS if any(matches(s, name) for s in selectors(bash_command))}


def reachable(downstream, start, goal):
    """Is there a path start → … → goal in the Airflow task graph?"""
    seen, queue = set(), deque([start])
    while queue:
        node = queue.popleft()
        if node in seen:
            continue
        seen.add(node)
        if node == goal:
            return True
        for nxt in downstream.get(node, ()):
            if nxt not in seen:
                queue.append(nxt)
    return False


def main():
    global MODELS
    MODELS = staging_models()

    dag_mod = load(f"{DAGS_DIR}/grocery_dbt.py", "gdbt_staging_order")
    dag = dag_mod.dag

    builders = {}          # staging model → [task_id, ...]
    task_selects = {}      # task_id → {staging models it builds}
    downstream = {}        # task_id → downstream task_ids
    for task in dag.tasks:
        downstream[task.task_id] = set(task.downstream_task_ids)
        if not DBT_RUN_RE.search(getattr(task, "bash_command", "") or ""):
            continue
        built = builds(task.bash_command)
        task_selects[task.task_id] = built
        for model in built:
            builders.setdefault(model, []).append(task.task_id)

    disk_edges = {(child, parent) for child, parents in MODELS.items()
                  for parent in parents if parent in MODELS}

    # ------------------------------------------------------------------
    # 1. the declaration and the SQL on disk agree, both ways
    # ------------------------------------------------------------------
    undeclared = sorted(disk_edges - set(INTRA_STAGING_REFS))
    check("1a every staging→staging ref on disk is declared in INTRA_STAGING_REFS",
          not undeclared,
          f"undeclared: {undeclared} — decide the build order for it, then declare it")

    stale = sorted(set(INTRA_STAGING_REFS) - disk_edges)
    check("1b every declared pair still exists on disk", not stale, f"stale: {stale}")

    self_refs = sorted(m for m, parents in MODELS.items() if m in parents)
    check("1c no staging model refs itself", not self_refs, str(self_refs))

    # A cycle has no topological order: dbt would fail the run outright, and no
    # task-level ordering could exist either. Assert it here so the failure names
    # the nodes instead of surfacing as a dbt error.
    cyclic = []
    for node in sorted(MODELS):
        stack, seen = [node], set()
        while stack:
            cur = stack.pop()
            for parent in MODELS.get(cur, ()):
                if parent == node and cur != node:
                    cyclic.append(f"{node} <- {cur}")
                elif parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
    check("1d the intra-staging ref graph is acyclic", not cyclic, str(sorted(set(cyclic))))

    # ------------------------------------------------------------------
    # 2. coverage — every staging model is built, exactly once
    # ------------------------------------------------------------------
    unbuilt = sorted(m for m in MODELS if m not in builders)
    check("2a every staging model on disk is built by some `dbt run` task",
          not unbuilt, f"no task builds {unbuilt}")

    doubles = sorted((m, t) for m, t in builders.items() if len(t) > 1)
    check("2b every staging model is built by exactly one `dbt run` task",
          not doubles, str(doubles))

    # ------------------------------------------------------------------
    # 3. the hazard's precondition — the layer is a view on Postgres, so it is
    #    rebuilt by the rename + `drop ... cascade` swap this card is about.
    #    (If staging ever becomes a table there is no cascade to lose a
    #    dependent to, and 4a's ordering requirement stops being load-bearing.)
    # ------------------------------------------------------------------
    project = yaml.safe_load(open(PROJECT_YML))
    materialization = (
        ((project.get("models") or {}).get("grocery") or {})
        .get(LAYER, {}).get("+materialized", "view")
    )
    # Two reasons this is asserted rather than assumed: AGENTS.md pins staging to
    # views by design, and a view is the only materialization with a dependent to
    # lose — a table swap does not drag its dependents along, so if staging ever
    # became tables 4a would stop being the property that protects it.
    check("3 staging is a view (AGENTS.md: views by design) — the swap 4a orders",
          materialization == "view",
          f"materialization={materialization!r} — re-read 4a: the rename + "
          f"`drop ... cascade` swap only loses dependents for a VIEW")

    # ------------------------------------------------------------------
    # 4. the ordering guarantee, per declared pair
    # ------------------------------------------------------------------
    unordered, uncovered = [], []
    for child, parent in sorted(INTRA_STAGING_REFS):
        child_tasks = builders.get(child, [])
        parent_tasks = builders.get(parent, [])
        ordered = False
        for child_task in child_tasks:
            for parent_task in parent_tasks:
                if child_task == parent_task:
                    # One dbt invocation for both: dbt builds the parent first.
                    ordered = True
                elif reachable(downstream, parent_task, goal=child_task):
                    # per-model tasks: the parent's task must finish first
                    ordered = True
        if not ordered:
            unordered.append(f"{parent} is not built before {child}")
        # ...and a task that rebuilds the dependent in a process that does NOT
        # also build its dependency would drop the dependency from under itself
        # the moment the dependency's own task runs (the t_b48af51f shape).
        uncovered += [(child, parent, t) for t in child_tasks
                      if parent not in task_selects.get(t, set())]

    check("4a every staging→staging ref is built dependency-first", not unordered,
          str(unordered))
    check("4b the task building a dependent also builds its dependency",
          not uncovered, str(uncovered))

    # ------------------------------------------------------------------
    # 5. the incident this card is about (t_b48af51f)
    # ------------------------------------------------------------------
    for child, parent in INTRA_STAGING_REFS:
        tasks = sorted(set(builders.get(child, [])) & set(builders.get(parent, [])))
        shared = (len(tasks) == 1 and child in task_selects.get(tasks[0], set())
                  and parent in task_selects.get(tasks[0], set()))
        check(f"5 {parent} and {child} are built by one invocation, "
              f"dependency first",
              shared,
              f"builders: {parent}={builders.get(parent)} {child}={builders.get(child)}")

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed:", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    main()
