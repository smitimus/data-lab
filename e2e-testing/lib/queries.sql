-- name: source_db_connection_check
SELECT 1 AS ok;

-- name: edw_connection_check
SELECT 1 AS ok;

-- name: source_schemas_exist
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('hr','pos','timeclock','ordering','fulfillment','transport','inv','pricing','control')
ORDER BY schema_name;

-- name: source_all_table_counts
SELECT table_schema, table_name,
       (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
FROM (
    SELECT table_schema, table_name,
           query_to_xml(format('SELECT count(*) AS cnt FROM %I.%I', table_schema, table_name), true, false, '') AS xml_count
    FROM information_schema.tables
    WHERE table_schema IN ('hr','pos','timeclock','ordering','fulfillment','transport','inv','pricing')
) t
ORDER BY table_schema, table_name;

-- name: check_table_has_rows
SELECT COUNT(*) AS cnt FROM {schema}.{table};

-- name: raw_all_schemas_exist
SELECT DISTINCT table_schema
FROM information_schema.tables
WHERE table_schema IN ('raw_hr','raw_pos','raw_timeclock','raw_ordering','raw_fulfillment','raw_transport','raw_inv','raw_pricing')
ORDER BY table_schema;

-- name: raw_all_table_counts
SELECT table_schema, table_name,
       (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
FROM (
    SELECT table_schema, table_name,
           query_to_xml(format('SELECT count(*) AS cnt FROM %I.%I', table_schema, table_name), true, false, '') AS xml_count
    FROM information_schema.tables
    WHERE table_schema IN ('raw_hr','raw_pos','raw_timeclock','raw_ordering','raw_fulfillment','raw_transport','raw_inv','raw_pricing')
) t
ORDER BY table_schema, table_name;

-- name: raw_tables_empty_check
SELECT table_schema, table_name
FROM (
    SELECT table_schema, table_name,
           (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
    FROM (
        SELECT table_schema, table_name,
               query_to_xml(format('SELECT count(*) AS cnt FROM %I.%I', table_schema, table_name), true, false, '') AS xml_count
        FROM information_schema.tables
        WHERE table_schema IN ('raw_hr','raw_pos','raw_timeclock','raw_ordering','raw_fulfillment','raw_transport','raw_inv','raw_pricing')
    ) t
) counts
WHERE row_count = 0;

-- name: raw_table_column_types
SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema = '{schema}' AND table_name = '{table}'
ORDER BY ordinal_position;

-- name: staging_views_exist
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'staging'
ORDER BY table_name;

-- name: staging_all_row_counts
SELECT table_name,
       (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
FROM (
    SELECT table_name,
           query_to_xml(format('SELECT count(*) AS cnt FROM staging.%I', table_name), true, false, '') AS xml_count
    FROM information_schema.views
    WHERE table_schema = 'staging'
) t
ORDER BY table_name;

-- name: mart_tables_exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'mart'
ORDER BY table_name;

-- name: mart_all_row_counts
SELECT table_name,
       (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
FROM (
    SELECT table_name,
           query_to_xml(format('SELECT count(*) AS cnt FROM mart.%I', table_name), true, false, '') AS xml_count
    FROM information_schema.tables
    WHERE table_schema = 'mart'
) t
ORDER BY table_name;

-- name: revenue_sum_check
SELECT
    (SELECT COALESCE(SUM(CAST(total AS numeric)), 0) FROM raw_pos.transactions) AS raw_total,
    (SELECT COALESCE(SUM(total), 0) FROM staging.stg_pos_transactions) AS staging_total,
    (SELECT COALESCE(SUM(pos_revenue), 0) FROM mart.mart_daily_revenue) AS mart_total,
    (SELECT COALESCE(SUM(total_revenue), 0) FROM mart.mart_daily_revenue) AS mart_total_revenue;

-- name: negative_value_check
SELECT 'raw_pos.transactions_total' AS check_name, COUNT(*) AS bad_count
FROM raw_pos.transactions WHERE CAST(total AS numeric) < 0
UNION ALL
SELECT 'raw_pos.transaction_items_line_total', COUNT(*)
FROM raw_pos.transaction_items WHERE CAST(line_total AS numeric) < 0
UNION ALL
SELECT 'raw_inv.stock_levels_qty', COUNT(*)
FROM raw_inv.stock_levels WHERE CAST(quantity_on_hand AS numeric) < 0
UNION ALL
SELECT 'mart.mart_inventory_summary_qty', COUNT(*)
FROM mart.mart_inventory_summary WHERE quantity_on_hand < 0;

-- name: count_propagation_transactions
SELECT 'raw_pos.transactions' AS layer, COUNT(*)::int AS row_count FROM raw_pos.transactions
UNION ALL
SELECT 'staging.stg_pos_transactions', COUNT(*)::int FROM staging.stg_pos_transactions
UNION ALL
SELECT 'mart.mart_daily_revenue(agg)', COUNT(*)::int FROM mart.mart_daily_revenue
UNION ALL
SELECT 'mart.mart_daily_revenue(sum_txn)', COALESCE(SUM(transaction_count), 0)::int FROM mart.mart_daily_revenue;

-- name: count_propagation_products
SELECT 'raw_pos.products' AS layer, COUNT(*)::int FROM raw_pos.products
UNION ALL
SELECT 'staging.stg_pos_products', COUNT(*)::int FROM staging.stg_pos_products;

-- name: count_propagation_locations
SELECT 'raw_hr.locations' AS layer, COUNT(*)::int FROM raw_hr.locations
UNION ALL
SELECT 'staging.stg_locations', COUNT(*)::int FROM staging.stg_locations;

-- name: count_propagation_employees
SELECT 'raw_hr.employees' AS layer, COUNT(*)::int FROM raw_hr.employees
UNION ALL
SELECT 'staging.stg_employees', COUNT(*)::int FROM staging.stg_employees;

-- name: count_propagation_departments
SELECT 'raw_pos.departments' AS layer, COUNT(*)::int FROM raw_pos.departments
UNION ALL
SELECT 'staging.stg_pos_departments', COUNT(*)::int FROM staging.stg_pos_departments;

-- name: count_propagation_transaction_items
SELECT 'raw_pos.transaction_items' AS layer, COUNT(*)::int FROM raw_pos.transaction_items
UNION ALL
SELECT 'staging.stg_pos_transaction_items', COUNT(*)::int FROM staging.stg_pos_transaction_items;

-- name: superset_mart_revenue_total
SELECT COALESCE(SUM(pos_revenue), 0) AS total_revenue FROM mart.mart_daily_revenue;

-- name: superset_mart_table_count
SELECT COUNT(*) AS table_count FROM information_schema.tables WHERE table_schema = 'mart';
