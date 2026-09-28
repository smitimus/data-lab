#!/usr/bin/env bash
# Offline tests for the CHART IDENTITY rule in superset/create_missing_dashboards.py.
#
# The seed builds each dashboard from a list of definitions ({dataset, name, viz,
# params}) and adopts an existing chart instead of re-creating it on a re-run. That
# adoption used to key on the chart NAME alone, and a name is not an identity: the
# Grocery Operations seed and the bundle both ship a "Labor Cost % of Revenue" on
# `mart_store_weekly_summary`, so dashboard 3's tile — whose definition asks for
# that name on `mart_labor_cost_by_department`, grouped by department — rendered a
# store-weekly metric grouped by location instead (t_0f87aab9). No gate can see
# that: the chart answers 200, its query_context is valid and the DOM renders a
# chart; the data is just from the wrong mart.
#
# The lookup is therefore NAME + DATASET, and it pages (Superset caps an effective
# page at 100 rows whatever page_size is asked, so a single request that "looks
# complete" never sees a chart past the 100th). Both are asserted here against the
# real module with a fake Superset chart API.
#
#   bash e2e-testing/test-seed-chart-identity.sh
#   SEED=/path/to/create_missing_dashboards.py bash e2e-testing/test-seed-chart-identity.sh
set -uo pipefail
H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SEED:-$(cd "$H/.." && pwd)/superset/create_missing_dashboards.py}"
WORK="$H/logs/seed-chart-identity"
rm -rf "$WORK"; mkdir -p "$WORK"

[ -f "$SRC" ] || { echo "seed not found at $SRC"; exit 1; }

PASS=0; FAIL=0
ok()  { echo "  PASS  $*"; PASS=$((PASS + 1)); }
bad() { echo "  FAIL  $*"; FAIL=$((FAIL + 1)); }
rc_is() { if [ "$2" = "$3" ]; then ok "$1: exit $2"; else bad "$1: exit $2 (want $3)"; fi; }
contains() { # label haystack needle
  if printf '%s' "$2" | grep -qF -- "$3"; then ok "$1: $3"; else bad "$1: missing '$3'"; fi
}
absent() { # label haystack needle
  if printf '%s' "$2" | grep -qF -- "$3"; then bad "$1: should not say '$3'"; else ok "$1: no '$3'"; fi
}

cat > "$WORK/driver.py" <<'PY'
"""Exercise create_chart()/_find_chart_id() against a fake Superset chart API."""
import importlib.util
import json
import sys
import types

SRC = sys.argv[1]
STATE = {"charts": [], "calls": [], "next_id": 900}
FAILURES = []
IGNORE_FILTER = False


def check(label, cond, detail=""):
    if cond:
        print("  PASS  %s" % label)
    else:
        FAILURES.append(label)
        print("  FAIL  %s%s" % (label, (": " + detail) if detail else ""))


class Resp:
    def __init__(self, status, payload, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


def _name_of(chart):
    return chart.get("slice_name")


def _rows_for(q):
    """Server-side name filter, exactly as /api/v1/chart/ serves it."""
    filters = q.get("filters") or []
    rows = STATE["charts"]
    if IGNORE_FILTER:
        # A build that ignores `filters` serves the whole listing instead: the
        # page walk below is what still finds the row (the Grocery Operations
        # seed's documented failure mode — its 500-row page silently hid the
        # oldest 'Stock Aging Breakdown').
        return rows
    for f in filters:
        if f.get("col") == "slice_name" and f.get("opr") == "eq":
            rows = [c for c in rows if _name_of(c) == f.get("value")]
    return rows


class FakeRequests:
    """Only the surface create_chart/_find_chart_id touch."""

    @staticmethod
    def get(url, headers=None, params=None, timeout=None):
        STATE["calls"].append(("GET", url, json.loads(params["q"]) if params else None))
        if url.endswith("/api/v1/chart/"):
            q = json.loads(params["q"])
            # Superset caps an EFFECTIVE page at 100 rows, whatever page_size is
            # asked for: a 500 asks for 500 and receives 100.
            size = min(int(q.get("page_size") or 100), 100)
            page = int(q.get("page") or 0)
            batch = _rows_for(q)[page * size:(page + 1) * size]
            return Resp(200, {"result": batch})
        return Resp(200, {"result": {}})  # dataset detail (main_dttm_col)

    @staticmethod
    def post(url, headers=None, json=None, timeout=None):  # noqa: A002
        body = json
        STATE["calls"].append(("POST", url, body))
        cid = STATE["next_id"]
        STATE["next_id"] += 1
        STATE["charts"].append({"id": cid, "slice_name": body["slice_name"],
                                "datasource_id": body["datasource_id"]})
        return Resp(201, {"id": cid, "result": {"slice_name": body["slice_name"]}})

    @staticmethod
    def put(url, headers=None, json=None, timeout=None):  # noqa: A002
        STATE["calls"].append(("PUT", url, json))
        return Resp(200, {"result": {}})


requests = types.ModuleType("requests")
requests.get = FakeRequests.get
requests.post = FakeRequests.post
requests.put = FakeRequests.put
sys.modules["requests"] = requests

# The seed builds a query_context through the shared helper; that is another
# module's contract (and it talks to Superset), so it is a stub here.
qc = types.ModuleType("_superset_query_context")
qc.build_query_context = lambda ds_id, params, token=None, base_url=None: "{}"
sys.modules["_superset_query_context"] = qc

spec = importlib.util.spec_from_file_location("cmd_under_test", SRC)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

URL = "http://superset:8088"
NAME = "Labor Cost % of Revenue"
DEPT_DS = 24        # mart_labor_cost_by_department
STORE_WEEKLY_DS = 10  # mart_store_weekly_summary (the Grocery Operations chart)
PARAMS = {"metrics": [mod.make_metric("labor_cost_pct_of_revenue", "AVG")],
          "groupby": ["department"], "row_limit": 20}


def posts():
    return [c for c in STATE["calls"] if c[0] == "POST"]


def puts():
    return [c for c in STATE["calls"] if c[0] == "PUT"]


def reset(charts):
    STATE["charts"] = charts
    STATE["calls"] = []


print("== a same-named chart on ANOTHER dataset is not adopted ==")
reset([{"id": 18, "slice_name": NAME, "datasource_id": STORE_WEEKLY_DS}])
c = mod.create_chart("tok", URL, DEPT_DS, NAME, "bar", dict(PARAMS))
check("the foreign row (id=18, ds=10) is not returned", c and c["id"] != 18,
      repr(c))
check("a chart IS created on the dataset the definition resolved",
      len(posts()) == 1 and posts()[0][2]["datasource_id"] == DEPT_DS)
check("...and it carries this definition's params, not the foreign chart's",
      posts() and json.loads(posts()[0][2]["params"])["groupby"] == ["department"])
check("the foreign chart is never written to (no PUT, no repair)",
      puts() == [], repr(puts()[:1]))

print()
print("== a chart of the same name ON THE SAME dataset is adopted (idempotent) ==")
reset([{"id": 40, "slice_name": NAME, "datasource_id": DEPT_DS}])
c = mod.create_chart("tok", URL, DEPT_DS, NAME, "bar", dict(PARAMS))
check("the same-dataset row is reused", c and c["id"] == 40, repr(c))
check("no chart is POSTed on a re-run", posts() == [])
check("the reused row is RE-ASSERTED, not returned untouched",
      len(puts()) == 1 and puts()[0][1].endswith("/api/v1/chart/40"), repr(puts()[:1]))
check("...and the re-assert carries this definition's params + query_context",
      puts() and "params" in puts()[0][2] and "query_context" in puts()[0][2])
check("...and never `dashboards` (a PUT with [] would UNLINK the chart)",
      puts() and "dashboards" not in puts()[0][2])

print()
print("== both exist: the same-dataset one wins, never the foreign one ==")
reset([{"id": 55, "slice_name": NAME, "datasource_id": DEPT_DS},
       {"id": 18, "slice_name": NAME, "datasource_id": STORE_WEEKLY_DS}])
c = mod.create_chart("tok", URL, DEPT_DS, NAME, "bar", dict(PARAMS))
check("the dataset-scoped row is reused", c and c["id"] == 55, repr(c))
check("no duplicate is created", posts() == [])
check("the foreign row is untouched even when both exist",
      all(not u[1].endswith("/api/v1/chart/18") for u in puts()), repr(puts()[:1]))
foreign = [c_ for c_ in STATE["calls"] if c_[0] == "GET"
           and c_[2] and c_[2].get("page") == 0]
check("the lookup asks the API for this name (server-side filter)",
      foreign and foreign[0][2]["filters"] == [
          {"col": "slice_name", "opr": "eq", "value": NAME}], repr(foreign[:1]))

print()
print("== a re-assert the API refuses is reported, and the tile is kept ==")
STATE["charts"] = [{"id": 40, "slice_name": NAME, "datasource_id": DEPT_DS}]
STATE["calls"] = []


class RefusePut(FakeRequests):
    @staticmethod
    def put(url, headers=None, json=None, timeout=None):  # noqa: A002
        STATE["calls"].append(("PUT", url, json))
        return Resp(422, {}, text="cannot update")


requests.put = RefusePut.put
c = mod.create_chart("tok", URL, DEPT_DS, NAME, "bar", dict(PARAMS))
requests.put = FakeRequests.put
check("the chart is still returned (the dashboard keeps its tile)",
      c and c["id"] == 40, repr(c))
check("no duplicate row is appended when the repair fails", posts() == [])

print()
print("== a chart past the first 100 rows is found (the listing pages) ==")
# An instance carries ~106 charts and grows: the row a re-run must reuse is not
# guaranteed to be on the first page. The name filter is the fast path; the page
# walk below is what still finds it when a build answers with the whole listing.
IGNORE_FILTER = True
filler = [{"id": i, "slice_name": "Chart %03d" % i, "datasource_id": 99}
          for i in range(1, 101)]
reset(filler + [{"id": 177, "slice_name": NAME, "datasource_id": DEPT_DS}])
c = mod.create_chart("tok", URL, DEPT_DS, NAME, "bar", dict(PARAMS))
check("the chart on page 2 is reused, not duplicated", c and c["id"] == 177, repr(c))
check("no duplicate chart row was appended", posts() == [])
pages = [c_[2]["page"] for c_ in STATE["calls"]
         if c_[0] == "GET" and c_[2] and "page" in c_[2]]
check("the lookup walked past page 0 (asked page 1)", 1 in pages, repr(pages))
check("the requested page size is the 100 the API actually serves",
      all(c_[2]["page_size"] == 100 for c_ in STATE["calls"]
          if c_[0] == "GET" and c_[2] and "page_size" in c_[2]))
# The control for the paging fix: the OLD single request (page_size 500, no page
# walk) sees only the first 100 rows — it cannot see the chart on row 101.
one = FakeRequests.get(URL + "/api/v1/chart/", params={"q": json.dumps({"page_size": 500})})
check("control: one page_size=500 request returns 100 rows and misses it",
      len(one.json()["result"]) == 100
      and all(r["id"] != 177 for r in one.json()["result"]))
IGNORE_FILTER = False

print()
print("== an unreadable chart API warns and does not kill the seed ==")
reset([])


class Boom(FakeRequests):
    @staticmethod
    def get(url, headers=None, params=None, timeout=None):
        return Resp(500, {}, text="boom")


requests.get = Boom.get
c = mod.create_chart("tok", URL, DEPT_DS, NAME, "bar", dict(PARAMS))
requests.get = FakeRequests.get
check("a 500 on the lookup still yields the chart the definition asked for",
      c is not None and len(posts()) == 1, repr(c))

print()
if FAILURES:
    print("driver: %d case(s) FAILED" % len(FAILURES))
    sys.exit(1)
print("driver: all cases passed")
PY

echo "== driver: create_chart/_find_chart_id against a fake chart API =="
OUT="$(python3 "$WORK/driver.py" "$SRC" 2>&1)"; RC=$?
printf '%s\n' "$OUT" | sed 's/^/    /'
rc_is driver "$RC" 0
contains driver "$OUT" "the foreign row (id=18, ds=10) is not returned"
contains driver "$OUT" "the chart on page 2 is reused, not duplicated"
contains driver "$OUT" "control: one page_size=500 request returns 100 rows and misses it"
contains driver "$OUT" "params/query_context re-asserted"
contains driver "$OUT" "Re-assert failed 'Labor Cost % of Revenue' (id=40): 422"

echo
echo "== the warning names the dataset it refused to adopt from =="
cat > "$WORK/warn.py" <<'PY'
import importlib.util, json, sys, types
SRC = sys.argv[1]
sys.argv = [SRC]


class Resp:
    def __init__(self, payload, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload


requests = types.ModuleType("requests")
requests.get = lambda url, headers=None, params=None, timeout=None: Resp(
    {"result": [{"id": 18, "slice_name": "Labor Cost % of Revenue",
                 "datasource_id": 10}]} if url.endswith("/api/v1/chart/") else {"result": {}})
requests.post = lambda *a, **k: Resp({"id": 900, "result": {}}, status_code=201)
requests.put = lambda *a, **k: Resp({"result": {}})
sys.modules["requests"] = requests
qc = types.ModuleType("_superset_query_context")
qc.build_query_context = lambda *a, **k: "{}"
sys.modules["_superset_query_context"] = qc
spec = importlib.util.spec_from_file_location("cmd", SRC)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod.create_chart("tok", "http://superset:8088", 24, "Labor Cost % of Revenue",
                 "bar", {"metrics": [mod.make_metric("labor_cost_pct_of_revenue", "AVG")],
                         "groupby": ["department"]})
PY
WARN="$(python3 "$WORK/warn.py" "$SRC" 2>&1)"; WRC=$?
rc_is warn-run "$WRC" 0
contains warn "$WARN" "also exists on other datasets [10] — not adopted, this definition asks for dataset 24"
contains warn "$WARN" "Chart 'Labor Cost % of Revenue' (id=900)"

echo
echo "== red control: the same driver against a NAME-ONLY lookup must fail =="
# The control for the whole test: strip the dataset comparison (the pre-fix
# behaviour) and the driver's first case has to go red. Without this, a driver
# that silently stopped looking could pass and certify nothing.
python3 - "$SRC" "$WORK/seed-name-only.py" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
body = open(src).read()
needle = 'if c.get("datasource_id") != ds_id:'
if needle not in body:
    sys.exit("the dataset comparison moved — update this red control")
open(dst, "w").write(body.replace(
    needle, 'if False:  # red control: name-only adoption, the pre-fix behaviour'))
PY
RED="$(python3 "$WORK/driver.py" "$WORK/seed-name-only.py" 2>&1)"; RRC=$?
rc_is red-control "$RRC" 1
contains red-control "$RED" "FAIL  the foreign row (id=18, ds=10) is not returned"
absent red-control "$RED" "driver: all cases passed"

echo
echo "== wiring: the seed passes the dataset it resolved into the lookup =="
grep -qF 'existing = _find_chart_id(token, base_url, slice_name, ds_id)' "$SRC" \
  && ok "create_chart scopes its lookup to ds_id" \
  || bad "create_chart still looks charts up by name alone"
grep -qF 'def _find_chart_id(token, base_url, slice_name, ds_id):' "$SRC" \
  && ok "_find_chart_id requires the dataset (no name-only default)" \
  || bad "_find_chart_id has no required ds_id parameter"
grep -qF 'if c.get("datasource_id") != ds_id:' "$SRC" \
  && ok "a foreign dataset is filtered out of the match" \
  || bad "the lookup does not compare datasource_id"
grep -qF 'update_payload = {k: v for k, v in payload.items() if k != "dashboards"}' "$SRC" \
  && ok "the reuse path re-asserts the payload minus 'dashboards'" \
  || bad "the reuse path returns the existing chart untouched"

echo
echo "test-seed-chart-identity.sh: $PASS passed, $FAIL failed"
[ "$FAIL" = "0" ] || exit 1
