import os
import json
import random
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import snowflake.connector
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CONFIG_PATH = Path("config.json")
DEFAULT_CONFIG = {
    "locations": {
        "Highland Village": {"sales_start_date": "2026-01-15", "opening_date": "2026-06-01"},
        "Lakeview": {"sales_start_date": "2026-01-20", "opening_date": "2026-07-01"},
    },
    "attrition": {"mode": "attrition_rate", "attrition_rate": 5.0, "avg_monthly_stay": 12},
}

FALLBACK_LOCATIONS = ["Highland Village", "Lakeview"]


def load_config():
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


_snowflake_available = False

_sf_env = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", "VSC78986.us-east-1"),
    "user": os.environ.get("SNOWFLAKE_USERNAME", "MIKEPRINCE"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "REVRYZE"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "INGEST_WH"),
}


def _run_snowflake_probe():
    import subprocess, sys
    try:
        result = subprocess.run(
            [sys.executable, "-c",
             "import snowflake.connector; "
             "snowflake.connector.connect("
             f"account='{_sf_env['account']}', "
             f"user='{_sf_env['user']}', "
             f"password='{_sf_env['password']}', "
             f"database='{_sf_env['database']}', "
             f"schema='{_sf_env['schema']}', "
             f"warehouse='{_sf_env['warehouse']}', "
             "login_timeout=5, network_timeout=10).close(); "
             "print('OK')"],
            capture_output=True, text=True, timeout=15
        )
        return result.stdout.strip() == "OK"
    except Exception:
        return False


def _probe_snowflake_loop():
    import time as _time
    global _snowflake_available
    _time.sleep(30)
    retry_intervals = [0, 60, 120, 300]
    for i, wait in enumerate(retry_intervals):
        if _snowflake_available:
            return
        if wait > 0:
            _time.sleep(wait)
        if _run_snowflake_probe():
            _snowflake_available = True
            return


threading.Thread(target=_probe_snowflake_loop, daemon=True).start()


def get_snowflake_conn():
    if not _snowflake_available:
        return None
    try:
        conn = snowflake.connector.connect(
            login_timeout=5, network_timeout=10,
            **_sf_env,
        )
        return conn
    except Exception:
        return None


def _reset_probe():
    global _snowflake_available
    if _run_snowflake_probe():
        _snowflake_available = True


@app.get("/api/reset-snowflake")
def reset_snowflake():
    global _snowflake_available
    _snowflake_available = False
    threading.Thread(target=_reset_probe, daemon=True).start()
    return {"status": "Snowflake connection cache cleared. Retrying in background."}


def generate_fallback_daily(location, start_date_str=None, end_date_str=None):
    seed_val = hash(location) % 1000
    rng = random.Random(seed_val)

    base_start = date(2026, 1, 15) if location == "Highland Village" else date(2026, 1, 20)
    base_end = date.today()

    if start_date_str:
        try:
            filter_start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            base_start = max(base_start, filter_start)
        except ValueError:
            pass
    if end_date_str:
        try:
            filter_end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
            base_end = min(base_end, filter_end)
        except ValueError:
            pass

    rows = []
    current = base_start
    while current <= base_end:
        ad_spend = round(rng.uniform(15, 55), 2)
        meta_leads = rng.randint(3, 15)
        memberships_sold = 1 if rng.random() < 0.25 else 0
        membership_revenue = memberships_sold * round(rng.uniform(149, 299), 2)
        rows.append({
            "report_date": current.isoformat(),
            "location_name": location,
            "ad_spend": ad_spend,
            "meta_leads": meta_leads,
            "memberships_sold": memberships_sold,
            "membership_revenue": round(membership_revenue, 2),
        })
        current += timedelta(days=1)
    return rows


@app.get("/api/locations")
def get_locations():
    conn = get_snowflake_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT LOCATION_NAME FROM REVRYZE.ANALYTICS.LOCATION_MAP ORDER BY LOCATION_NAME")
            rows = cur.fetchall()
            return [r[0] for r in rows]
        except Exception:
            pass
        finally:
            conn.close()
    return FALLBACK_LOCATIONS


@app.get("/api/summary")
def get_summary(location: str, start_date: str = None, end_date: str = None):
    conn = get_snowflake_conn()
    snowflake_ok = False

    if conn:
        try:
            cur = conn.cursor()
            query = """
                SELECT
                    COALESCE(SUM(DAILY_SPEND), 0) AS total_ad_spend,
                    COALESCE(SUM(DAILY_LEADS), 0) AS total_meta_leads,
                    COALESCE(SUM(DAILY_MEMBERSHIPS_SOLD), 0) AS memberships_sold,
                    COALESCE(SUM(DAILY_REVENUE), 0) AS total_membership_revenue
                FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY
                WHERE LOCATION_NAME = %s
            """
            params = [location]
            if start_date:
                query += " AND DATE >= %s"
                params.append(start_date)
            if end_date:
                query += " AND DATE <= %s"
                params.append(end_date)

            cur.execute(query, params)
            row = cur.fetchone()
            total_ad_spend = float(row[0])
            total_meta_leads = int(row[1])
            memberships_sold = int(row[2])
            total_membership_revenue = float(row[3])
            snowflake_ok = True
        except Exception:
            pass
        finally:
            conn.close()

    if not snowflake_ok:
        daily_rows = generate_fallback_daily(location, start_date, end_date)
        total_ad_spend = sum(r["ad_spend"] for r in daily_rows)
        total_meta_leads = sum(r["meta_leads"] for r in daily_rows)
        memberships_sold = sum(r["memberships_sold"] for r in daily_rows)
        total_membership_revenue = sum(r["membership_revenue"] for r in daily_rows)

    cpl = round(total_ad_spend / total_meta_leads, 2) if total_meta_leads > 0 else 0
    cost_per_membership = round(total_ad_spend / memberships_sold, 2) if memberships_sold > 0 else 0

    cfg = load_config()
    loc_cfg = cfg.get("locations", {}).get(location, {})
    sales_start = loc_cfg.get("sales_start_date")

    days_selling = 0
    memberships_per_day = 0
    if sales_start:
        sales_start_dt = datetime.strptime(sales_start, "%Y-%m-%d").date()
        days_selling = max(1, (date.today() - sales_start_dt).days)
        memberships_per_day = round(memberships_sold / days_selling, 3)

    return {
        "location": location,
        "total_ad_spend": round(total_ad_spend, 2),
        "total_meta_leads": total_meta_leads,
        "memberships_sold": memberships_sold,
        "total_membership_revenue": round(total_membership_revenue, 2),
        "cpl": cpl,
        "cost_per_membership": cost_per_membership,
        "days_selling": days_selling,
        "memberships_per_day": memberships_per_day,
        "data_source": "snowflake" if snowflake_ok else "sample",
    }


@app.get("/api/daily")
def get_daily(location: str, start_date: str = None, end_date: str = None):
    conn = get_snowflake_conn()
    if conn:
        try:
            cur = conn.cursor()
            query = """
                SELECT
                    LOCATION_NAME,
                    DATE AS REPORT_DATE,
                    DAILY_SPEND AS AD_SPEND,
                    DAILY_LEADS AS META_LEADS,
                    DAILY_MEMBERSHIPS_SOLD AS MEMBERSHIPS_SOLD,
                    DAILY_REVENUE AS MEMBERSHIP_REVENUE,
                    CUMULATIVE_MEMBERSHIPS,
                    CUMULATIVE_SPEND
                FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY
                WHERE LOCATION_NAME = %s
            """
            params = [location]
            if start_date:
                query += " AND DATE >= %s"
                params.append(start_date)
            if end_date:
                query += " AND DATE <= %s"
                params.append(end_date)
            query += " ORDER BY DATE ASC"
            cur.execute(query, params)

            columns = [desc[0].lower() for desc in cur.description]
            rows = cur.fetchall()
            result = []
            for row in rows:
                record = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if isinstance(val, (date, datetime)):
                        val = val.isoformat()[:10]
                    elif isinstance(val, bytes):
                        val = val.decode("utf-8")
                    elif isinstance(val, Decimal):
                        val = float(val)
                    record[col] = val
                result.append(record)
            return result
        except Exception:
            pass
        finally:
            conn.close()

    rows = generate_fallback_daily(location, start_date, end_date)
    for r in rows:
        r["data_source"] = "sample"
    return rows


@app.get("/api/config")
def get_config():
    return load_config()


@app.post("/api/config")
async def post_config(request: Request):
    body = await request.json()
    save_config(body)
    return body


app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")


@app.middleware("http")
async def add_no_cache(request: Request, call_next):
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.get("/")
def serve_index():
    return FileResponse("frontend/index.html")
