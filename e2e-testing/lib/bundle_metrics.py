#!/usr/bin/env python3
"""
Assert that a Superset dashboard bundle's pie / big_number charts actually say
WHICH metric they plot — the singular `params.metric` their plugin reads.

WHY THIS EXISTS
---------------
A pie / big_number / big_number_total chart whose params carry only the plural
`metrics` sends an `orderby` built from a `metric` key that is not there. The
Echarts pie plugin builds

    orderby: [[metric, false]]        (only when sort_by_metric is set)

so it sends `orderby: [[null, false]]`, Superset's /api/v1/chart/data validator
answers

    400  Request is incorrect:
         {'queries': {0: {'orderby': {0: {0: ['Field may not be null.']}}}}}

and that tile renders "Unexpected error" forever — while the chart's stored
query_context is valid, the API 200s, and every DB gate stays green. It is
invisible to every check except the browser DOM.

This is the offline, stdlib-only gate over the BUNDLE (the artifact that ships):
`superset/dashboards/normalise_chart_metrics.py` is the repair, and this is the
assertion that stops a re-exported bundle from silently re-introducing the
shape. It parses with `re`, not yaml, on purpose: the guests that run the e2e
suite have no PyYAML, and install.sh parses the bundle the same way.

Usage:
    python3 e2e-testing/lib/bundle_metrics.py BUNDLE.zip [BUNDLE.zip ...]

Exit: 0 every chart in the family carries the singular metric
      1 at least one does not (listed on stderr as `VIOLATION`)
      2 no bundle could be read / no chart at all was found
"""

import re
import sys
import zipfile

SINGULAR_METRIC_VIZ = ("pie", "big_number", "big_number_total")

VIZ_RE = re.compile(r"(?m)^viz_type:[ \t]*['\"]?([A-Za-z_]+)")
# `params:` to the next top-level key: every line of the block is indented.
PARAMS_RE = re.compile(r"(?m)^params:\n((?:[ \t].*\n?)*)")
METRIC_KEY_RE = re.compile(r"(?m)^[ \t]+metric:[ \t]*(.*)$")
METRICS_KEY_RE = re.compile(r"(?m)^[ \t]+metrics:")
NULLISH = ("", "null", "~", "''", '""', "Null", "NULL")


def params_block(text):
    """The chart's params block as text, or '' when the file has none."""
    m = PARAMS_RE.search(text)
    return m.group(1) if m else ""


def singular_metric_present(block, indent):
    """
    True when `metric:` is present at the params-key indent and has a value —
    either on the same line (`metric: {…}` / a scalar) or as an indented block
    (`metric:` then `    expressionType: SIMPLE`). A bare `metric: null` is not
    a metric.
    """
    for m in re.finditer(r"(?m)^(?P<i>[ \t]+)metric:[ \t]*(?P<v>.*)$", block):
        if len(m.group("i")) != indent:
            continue
        value = m.group("v").strip()
        if value not in NULLISH:
            return True
        rest = block[m.end():]
        nxt = re.match(r"\n(?P<i>[ \t]+)\S", rest)
        if nxt and len(nxt.group("i")) > indent:
            return True
    return False


def plural_metric_present(block, indent):
    """True when the plural `metrics:` key sits at the params-key indent."""
    for m in re.finditer(r"(?m)^(?P<i>[ \t]+)metrics:", block):
        if len(m.group("i")) == indent:
            return True
    return False


def check_bundle(path):
    """Return (family, checked, violations) for one bundle."""
    family = checked = 0
    violations = []
    with zipfile.ZipFile(path) as z:
        names = sorted(n for n in z.namelist() if "/charts/" in n and n.endswith(".yaml"))
        if not names:
            raise ValueError("no charts/*.yaml entries — not a Superset bundle?")
        for name in names:
            text = z.read(name).decode("utf-8")
            m = VIZ_RE.search(text)
            viz_type = m.group(1) if m else "?"
            if viz_type not in SINGULAR_METRIC_VIZ:
                continue
            family += 1
            block = params_block(text)
            if not block:
                violations.append("%s: viz_type=%s has no params block" % (name, viz_type))
                continue
            checked += 1
            indent = len(block) - len(block.lstrip(" \t"))
            if not singular_metric_present(block, indent):
                violations.append(
                    "%s: viz_type=%s plots no metric — add params.metric (%s)"
                    % (name.rsplit("/", 1)[-1], viz_type, "the plural metrics is not enough"
                       if plural_metric_present(block, indent) else "params.metrics/params.metric")
                )
            elif plural_metric_present(block, indent):
                violations.append(
                    "%s: viz_type=%s keeps the plural metrics next to the singular one"
                    % (name.rsplit("/", 1)[-1], viz_type)
                )
    return family, checked, violations


def main(argv):
    if len(argv) < 2:
        print("usage: bundle_metrics.py BUNDLE.zip [BUNDLE.zip ...]", file=sys.stderr)
        return 2
    rc = 0
    for path in argv[1:]:
        try:
            family, checked, violations = check_bundle(path)
        except (OSError, ValueError, zipfile.BadZipFile) as exc:
            print("UNREADABLE %s: %s" % (path, exc), file=sys.stderr)
            rc = 2
            continue
        print("%s: %d chart(s) in the singular-metric family, %d read"
              % (path.rsplit("/", 1)[-1], family, checked))
        for line in violations:
            print("  VIOLATION  %s" % line, file=sys.stderr)
        if violations:
            rc = rc or 1
        elif family == 0:
            print("  no pie/big_number chart in this bundle — nothing to assert", file=sys.stderr)
    if rc == 0:
        print("OK: every pie/big_number chart in every bundle carries the singular metric")
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv))
