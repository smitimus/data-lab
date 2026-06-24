#!/usr/bin/env python3
import time
from typing import Any, Dict, List, Optional
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

 
def _resp_ok_json(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except ValueError:
        return None

def _make_success(data: Any) -> Dict[str, Any]:
    return {"success": True, "data": data, "error": None}

def _make_error(error: str) -> Dict[str, Any]:
    return {"success": False, "data": None, "error": error}

 
def airflow_health(base_url: str, user: str, password: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/health"
    try:
        resp = requests.get(url, timeout=30, auth=HTTPBasicAuth(user, password))
        if resp.ok:
            data = _resp_ok_json(resp)
            return _make_success(data)
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def airflow_unpause_dag(base_url: str, user: str, password: str, dag_id: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}"
    payload = {"is_paused": False}
    try:
        resp = requests.patch(url, json=payload, timeout=30, auth=HTTPBasicAuth(user, password))
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def airflow_trigger_dag(base_url: str, user: str, password: str, dag_id: str, conf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    payload = {"conf": conf or {}}
    try:
        resp = requests.post(url, json=payload, timeout=30, auth=HTTPBasicAuth(user, password))
        if resp.ok:
            j = _resp_ok_json(resp)
            dag_run_id = None
            if isinstance(j, dict):
                dag_run_id = j.get("dag_run_id") or j.get("dagRunId")
            return _make_success({"dag_run_id": dag_run_id, "response": j})
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def airflow_get_dag_run(base_url: str, user: str, password: str, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
    try:
        resp = requests.get(url, timeout=30, auth=HTTPBasicAuth(user, password))
        if resp.ok:
            j = _resp_ok_json(resp)
            return _make_success(j)
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def airflow_wait_for_dag(base_url: str, user: str, password: str, dag_id: str, timeout_minutes: int = 30) -> Dict[str, Any]:
    end_time = time.time() + timeout_minutes * 60
    while time.time() < end_time:
        # Try to fetch the latest runs and infer state from most recent one if possible
        found = airflow_list_dag_runs(base_url, user, password, dag_id, limit=1)
        if found.get("success"):
            runs = found.get("data") or []
            if isinstance(runs, list) and len(runs) > 0:
                run = runs[0]
                state = run.get("state")
                if state in ("success", "failed"):
                    return _make_success({"state": state, "dag_id": dag_id, "latest": run})
        time.sleep(30)
    return _make_error("Timeout waiting for Airflow DAG to complete")


def airflow_list_dag_runs(base_url: str, user: str, password: str, dag_id: str, limit: int = 5) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns?limit={limit}"
    try:
        resp = requests.get(url, timeout=30, auth=HTTPBasicAuth(user, password))
        if resp.ok:
            j = _resp_ok_json(resp)
            data = None
            if isinstance(j, dict):
                data = j.get("dag_runs") or j.get("data") or []
            elif isinstance(j, list):
                data = j
            else:
                data = []
            return _make_success(data)
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


# ---------------- B. Superset helpers ----------------
def superset_login(base_url: str, username: str, password: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/security/login"
    payload = {"username": username, "password": password, "provider": "db"}
    try:
        resp = requests.post(url, json=payload, timeout=30)
        if resp.ok:
            j = _resp_ok_json(resp) or {}
            token = j.get("access_token") or j.get("token")
            return _make_success({"access_token": token, "response": j})
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def superset_health(base_url: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/health"
    try:
        resp = requests.get(url, timeout=30)
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def _superset_headers(token: Optional[str]) -> Dict[str, str]:
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def superset_get_databases(base_url: str, token: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/database/"
    try:
        resp = requests.get(url, timeout=30, headers=_superset_headers(token))
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def superset_get_datasets(base_url: str, token: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/dataset/"
    try:
        resp = requests.get(url, timeout=30, headers=_superset_headers(token))
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def superset_get_charts(base_url: str, token: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/chart/"
    try:
        resp = requests.get(url, timeout=30, headers=_superset_headers(token))
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def superset_get_chart_data(base_url: str, token: str, chart_id: int) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/chart/{chart_id}/data"
    try:
        resp = requests.get(url, timeout=30, headers=_superset_headers(token))
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def superset_execute_sql(base_url: str, token: str, database_id: int, sql_text: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/sqllab/execute"
    payload = {"database_id": database_id, "sql": sql_text}
    try:
        resp = requests.post(url, json=payload, timeout=30, headers=_superset_headers(token))
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


# ---------------- C. Verisim API helpers ----------------
def verisim_health(base_url: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/grocery/generator/status"
    try:
        resp = requests.get(url, timeout=30)
        if resp.ok:
            data = _resp_ok_json(resp)
            # Best effort: return raw data but also try to surface key indicators
            surface = None
            if isinstance(data, dict):
                for k in ("state", "is_running", "running"):
                    if k in data:
                        surface = data.get(k)
                        break
            if surface is not None:
                return _make_success({"status": surface, "raw": data})
            return _make_success(data)
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def verisim_get_data(base_url: str, path: str, start_dt: Optional[str] = None, end_dt: Optional[str] = None, limit: int = 1000) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    try:
        while True:
            params = {"limit": limit, "offset": offset}
            if start_dt:
                params["start_dt"] = start_dt
            if end_dt:
                params["end_dt"] = end_dt
            resp = requests.get(url, timeout=30, params=params)
            if not resp.ok:
                return _make_error(f"HTTP {resp.status_code}: {resp.text}")
            data = resp.json()
            page = []
            if isinstance(data, list):
                page = data
            elif isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    page = data["data"]
                elif "rows" in data and isinstance(data["rows"], list):
                    page = data["rows"]
                else:
                    # Fall back to empty or single-item data
                    page = data.get("results") if isinstance(data.get("results"), list) else []  # type: ignore
            if not isinstance(page, list):
                page = [page]  # type: ignore
            all_rows.extend(page)  # type: ignore
            if len(page) < limit:
                break
            offset += limit
        return _make_success(all_rows)
    except Exception as e:
        return _make_error(str(e))


def verisim_start_generator(base_url: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/grocery/generator/start"
    try:
        resp = requests.post(url, timeout=30)
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


def verisim_stop_generator(base_url: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/grocery/generator/stop"
    try:
        resp = requests.post(url, timeout=30)
        if resp.ok:
            return _make_success(_resp_ok_json(resp))
        return _make_error(f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return _make_error(str(e))


# ---------------- D. Database helpers ----------------
def edw_connect(host: str, port: int, dbname: str, user: str, password: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, cursor_factory=RealDictCursor)
    return conn


def source_db_connect(host: str, port: int, dbname: str, user: str, password: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, cursor_factory=RealDictCursor)
    return conn


def db_get_row_count(conn: psycopg2.extensions.connection, schema: str, table: str) -> int:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        sql_query = sql.SQL('SELECT COUNT(*) as cnt FROM {}.{}').format(
            sql.Identifier(schema), sql.Identifier(table)
        )
        cur.execute(sql_query)
        row = cur.fetchone()
        return int(row.get("cnt", 0)) if row else 0


def db_get_schemas(conn: psycopg2.extensions.connection) -> List[str]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT schema_name FROM information_schema.schemata ORDER BY schema_name')
        rows = cur.fetchall()
        return [r.get("schema_name") for r in rows]


def db_get_tables(conn: psycopg2.extensions.connection, schema: str) -> List[str]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=%s', (schema,))
        rows = cur.fetchall()
        return [r.get("table_name") for r in rows]


def db_run_query(conn: psycopg2.extensions.connection, sql_text: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql_text)
        # If it's a SELECT-like statement, fetch results
        if cur.description:
            return cur.fetchall()
        else:
            return []


if __name__ == "__main__":
    funcs = [k for k in dir() if k.startswith(('airflow','superset','verisim','edw','source','db_'))]
    print(f"api-helpers.py loaded - {len(funcs)} symbols: {', '.join(funcs)}")
