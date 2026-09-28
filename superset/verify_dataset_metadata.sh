#!/usr/bin/env bash
# verify_dataset_metadata.sh — gate: every Superset dataset in the mart schema must
# carry the same column list as the physical table in the EDW.
#
# Why this exists
# ---------------
# A Superset dataset's column list is a SNAPSHOT taken at registration; nothing
# re-reads it on its own. The marts are CTAS-built on every dbt run, so a mart
# gaining (or renaming) a column is a routine event — after which the dataset is
# stale, and that staleness is INVISIBLE to every other gate:
#
#   * `/api/v1/chart/<id>/data/` still returns 200 with data,
#   * the null-datasource / null-query_context checks still pass,
#   * only the browser tile errors ("Unexpected error" / "Columns missing in dataset"),
#     which is why the headless DOM scan kept being the first thing to see it.
#
# The fix lives in the seeds (`_superset_dataset_metadata.py`: every seed refreshes
# the datasets it binds charts to). This gate is what proves the refresh happened,
# and what catches an instance whose metadata went stale by any other route.
#
# It reads both databases through the running postgres container — no API, no
# chart — so it works even while Superset itself is unhealthy.
#
# Exit codes: 0 clean (or nothing to compare yet), 1 stale datasets, 2 unreadable DB.
#
# Usage:
#   bash superset/verify_dataset_metadata.sh
#   EDW_DB=grocery SUP_DB=superset MART_SCHEMA=mart bash superset/verify_dataset_metadata.sh
#
# Test hooks — point these at pre-made lists instead of querying (see
# e2e-testing/test-dataset-metadata-gate.sh):
#   EDW_LIST      "table|column" per line, the EDW's columns
#   SUP_LIST      "table|column" per line, the dataset's registered columns
#   SUP_DS_LIST   "table|dataset_id" per line, every dataset (even a column-less one)

set -uo pipefail
export LC_ALL=C

EDW_DB="${EDW_DB:-grocery}"          # the DB holding the mart schema
SUP_DB="${SUP_DB:-superset}"         # Superset's own meta DB
MART_SCHEMA="${MART_SCHEMA:-mart}"

EDW_LIST="${EDW_LIST:-}"
SUP_LIST="${SUP_LIST:-}"
SUP_DS_LIST="${SUP_DS_LIST:-}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
edw_f="$tmpdir/edw"; sup_f="$tmpdir/sup"; ds_f="$tmpdir/ds"

# shellcheck disable=SC2016
query() {  # query <db> <sql-file-description> <sql>
    local db="$1" desc="$2" sql="$3" out="$4"
    if ! docker exec postgres psql -U postgres -d "$db" -tAc "$sql" > "$out" 2>"$tmpdir/err"; then
        echo "✗ CANNOT READ $desc ($db):"
        sed 's/^/    /' "$tmpdir/err"
        exit 2
    fi
}

# ---------------------------------------------------------------- read the three lists
if [ -n "$EDW_LIST" ]; then cp "$EDW_LIST" "$edw_f"; else
    query "$EDW_DB" "the EDW, schema $MART_SCHEMA" "
        select table_name || '|' || column_name
        from information_schema.columns
        where table_schema = '$MART_SCHEMA'" "$edw_f"
fi
if [ -n "$SUP_LIST" ]; then cp "$SUP_LIST" "$sup_f"; else
    query "$SUP_DB" "the Superset dataset columns" "
        select t.table_name || '|' || c.column_name
        from tables t
        join table_columns c on c.table_id = t.id
        where t.schema = '$MART_SCHEMA'" "$sup_f"
fi
if [ -n "$SUP_DS_LIST" ]; then cp "$SUP_DS_LIST" "$ds_f"; else
    # Listed separately from the column join on purpose: a dataset that exposes NO
    # columns would otherwise disappear from the comparison instead of failing it.
    query "$SUP_DB" "the Superset dataset list" "
        select t.table_name || '|' || t.id
        from tables t
        where t.schema = '$MART_SCHEMA'" "$ds_f"
fi

for f in "$edw_f" "$sup_f" "$ds_f"; do
    grep -v '^$' "$f" > "$f.clean" || true
    mv "$f.clean" "$f"
    sort -u -o "$f" "$f"
done

n_edw_tables=$(cut -d'|' -f1 "$edw_f" | sort -u | grep -c . || true)
n_ds=$(grep -c . "$ds_f" || true)

echo "=== dataset column metadata vs the EDW (schema $MART_SCHEMA) ==="
echo "EDW mart tables: $n_edw_tables   Superset mart datasets: $n_ds"

if [ "$n_ds" -eq 0 ]; then
    echo "SKIP: no Superset dataset in '$MART_SCHEMA' yet — nothing to compare"
    exit 0
fi

cut -d'|' -f1 "$edw_f" | sort -u > "$tmpdir/edw_tables"
cut -d'|' -f1 "$ds_f"  | sort -u > "$tmpdir/ds_tables"

# Two directed differences. Every line is "<table>|<column>" and both files are
# sorted the same way, so each direction stays grouped by table.
comm -23 "$edw_f" "$sup_f" > "$tmpdir/missing"   # the EDW has it, the dataset does not
comm -13 "$edw_f" "$sup_f" > "$tmpdir/extra"     # the dataset has it, the EDW does not

: > "$tmpdir/stale"
while IFS='|' read -r tbl ds_id; do
    [ -n "$tbl" ] || continue
    if ! grep -qxF "$tbl" "$tmpdir/edw_tables"; then
        echo "$tbl" >> "$tmpdir/stale"
        echo "  ✗ $tbl (dataset $ds_id): no such table in the EDW any more"
        continue
    fi
    miss=$(awk -F'|' -v t="$tbl" '$1==t{print $2}' "$tmpdir/missing" | paste -sd, -)
    extra=$(awk -F'|' -v t="$tbl" '$1==t{print $2}' "$tmpdir/extra" | paste -sd, -)
    if [ -z "$miss" ] && [ -z "$extra" ]; then
        continue
    fi
    echo "$tbl" >> "$tmpdir/stale"
    echo "  ✗ $tbl (dataset $ds_id):"
    [ -n "$miss" ]  && echo "      not in the dataset, the EDW has: ${miss}"
    [ -n "$extra" ] && echo "      stale in the dataset, the EDW dropped/renamed: ${extra}"
done < "$ds_f"
n_stale=$(grep -c . "$tmpdir/stale" || true)

# Informational: an EDW mart with no dataset is not a column disagreement (the
# MetricFlow time spine lives in schema `mart` and is deliberately not a BI
# dataset) — but a mart that LOSES its dataset is a broken dashboard, so say it.
n_unreg=0
while read -r tbl; do
    [ -n "$tbl" ] || continue
    if ! grep -qxF "$tbl" "$tmpdir/ds_tables"; then
        echo "  note: EDW mart without a Superset dataset: $tbl"
        n_unreg=$((n_unreg + 1))
    fi
done < "$tmpdir/edw_tables"

echo
if [ "$n_stale" -gt 0 ]; then
    echo "STALE: $n_stale of $n_ds dataset(s) disagree with the EDW — charts on them can"
    echo "       400 in the browser while every API and DB gate stays green."
    echo "       heal: docker compose -f superset/compose.yaml up -d --force-recreate superset-setup"
    echo "       or:   docker exec superset python3 /app/superset_home/_superset_dataset_metadata.py --refresh-all"
    exit 1
fi
echo "OK: all $n_ds dataset(s) in '$MART_SCHEMA' match the EDW column-for-column"
[ "$n_unreg" -gt 0 ] && echo "    ($n_unreg EDW mart(s) have no dataset — see the notes above)"
exit 0
