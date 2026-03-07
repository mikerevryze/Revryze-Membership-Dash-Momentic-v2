import os
import json
import random
import time
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
        "Highland Village": {"sales_start_date": "2026-01-15", "opening_date": "2026-06-01", "selected_campaigns": []},
        "Lakeview": {"sales_start_date": "2026-01-20", "opening_date": "2026-07-01", "selected_campaigns": []},
    },
    "attrition": {"mode": "attrition_rate", "attrition_rate": 5.0, "avg_monthly_stay": 12},
}

FALLBACK_LOCATIONS = ["Highland Village", "Lakeview"]


def load_config():
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)
    changed = False
    for loc_name, loc_cfg in cfg.get("locations", {}).items():
        if "selected_campaigns" not in loc_cfg:
            loc_cfg["selected_campaigns"] = []
            changed = True
    if changed:
        save_config(cfg)
    return cfg


def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


_snowflake_available = False

_sf_env = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", "VSC78986.us-east-1"),
    "user": os.environ.get("SNOWFLAKE_USERNAME", "MIKEPRINCE"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", "ChavezDog16!!!"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "REVRYZE"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "INGEST_WH"),
}

print(f"[STARTUP] SNOWFLAKE_PASSWORD source: {'env var' if os.environ.get('SNOWFLAKE_PASSWORD') else 'fallback'}")
print(f"[STARTUP] SNOWFLAKE_PASSWORD length: {len(_sf_env['password'])}")
print(f"[STARTUP] Snowflake account: {_sf_env['account']}, user: {_sf_env['user']}")


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
            capture_output=True, text=True, timeout=30
        )
        print(f"[PROBE] stdout={result.stdout.strip()!r}, stderr={result.stderr.strip()[:200]!r}")
        return result.stdout.strip() == "OK"
    except Exception as e:
        print(f"[PROBE] Exception: {e}")
        return False


def _probe_snowflake_loop():
    import time as _time
    global _snowflake_available
    _time.sleep(5)
    retry_intervals = [0, 30, 60, 120, 300]
    for i, wait in enumerate(retry_intervals):
        if _snowflake_available:
            return
        if wait > 0:
            _time.sleep(wait)
        print(f"[PROBE] Attempt {i+1}/{len(retry_intervals)}")
        if _run_snowflake_probe():
            _snowflake_available = True
            print("[PROBE] Snowflake connected successfully!")
            return
    print("[PROBE] All attempts exhausted, Snowflake not available")


threading.Thread(target=_probe_snowflake_loop, daemon=True).start()


def get_snowflake_conn(retries=5, retry_delay=2):
    if not _snowflake_available:
        return None
    for attempt in range(retries):
        try:
            conn = snowflake.connector.connect(
                login_timeout=15, network_timeout=15,
                **_sf_env,
            )
            return conn
        except Exception as e:
            if attempt < retries - 1:
                print(f"[CONN] Attempt {attempt+1}/{retries} failed: {e}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"[CONN] All {retries} attempts failed: {e}")
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


@app.on_event("startup")
def warmup_snowflake():
    def _warmup():
        global _snowflake_available
        for i in range(30):
            if _snowflake_available:
                break
            time.sleep(1)
        if not _snowflake_available:
            print("[WARMUP] Snowflake not available yet, skipping warmup")
            return
        try:
            conn = snowflake.connector.connect(
                login_timeout=15, network_timeout=15,
                **_sf_env,
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            print("[WARMUP] Warehouse warmed up with SELECT 1")
            conn.close()
        except Exception as e:
            print(f"[WARMUP] Failed to warm warehouse: {e}")
    threading.Thread(target=_warmup, daemon=True).start()


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


@app.get("/api/campaigns")
def get_campaigns(location: str):
    conn = get_snowflake_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT CAMPAIGN_NAME FROM REVRYZE.ANALYTICS.META_ADS "
                "WHERE LOCATION_NAME = %s AND CAMPAIGN_NAME IS NOT NULL "
                "ORDER BY CAMPAIGN_NAME ASC",
                [location],
            )
            return [r[0] for r in cur.fetchall()]
        except Exception:
            pass
        finally:
            conn.close()
    return []


def _parse_campaigns(campaigns_str):
    if not campaigns_str:
        return []
    return [c.strip() for c in campaigns_str.split(",") if c.strip()]


@app.get("/api/summary")
def get_summary(location: str, start_date: str = None, end_date: str = None, campaigns: str = None):
    campaign_list = _parse_campaigns(campaigns)
    conn = get_snowflake_conn()
    snowflake_ok = False

    if conn:
        try:
            cur = conn.cursor()
            if campaign_list:
                placeholders = ", ".join(["%s"] * len(campaign_list))
                meta_query = f"""
                    SELECT
                        COALESCE(SUM(SPEND), 0),
                        COALESCE(SUM(LEADS), 0)
                    FROM REVRYZE.ANALYTICS.META_ADS
                    WHERE LOCATION_NAME = %s
                      AND CAMPAIGN_NAME IN ({placeholders})
                """
                meta_params = [location] + campaign_list
                if start_date:
                    meta_query += " AND DATE_START >= %s"
                    meta_params.append(start_date)
                if end_date:
                    meta_query += " AND DATE_START <= %s"
                    meta_params.append(end_date)
                cur.execute(meta_query, meta_params)
                meta_row = cur.fetchone()
                total_ad_spend = float(meta_row[0])
                total_meta_leads = int(meta_row[1])

                ghl_query = """
                    SELECT
                        COALESCE(SUM(DAILY_MEMBERSHIPS_SOLD), 0),
                        COALESCE(SUM(DAILY_REVENUE), 0)
                    FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY
                    WHERE LOCATION_NAME = %s
                """
                ghl_params = [location]
                if start_date:
                    ghl_query += " AND DATE >= %s"
                    ghl_params.append(start_date)
                if end_date:
                    ghl_query += " AND DATE <= %s"
                    ghl_params.append(end_date)
                cur.execute(ghl_query, ghl_params)
                ghl_row = cur.fetchone()
                memberships_sold = int(ghl_row[0])
                total_membership_revenue = float(ghl_row[1])
            else:
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
def get_daily(location: str, start_date: str = None, end_date: str = None, campaigns: str = None):
    campaign_list = _parse_campaigns(campaigns)
    conn = get_snowflake_conn()
    if conn:
        try:
            cur = conn.cursor()
            if campaign_list:
                placeholders = ", ".join(["%s"] * len(campaign_list))
                query = f"""
                    SELECT
                        d.LOCATION_NAME,
                        d.DATE AS REPORT_DATE,
                        COALESCE(m.AD_SPEND, 0) AS AD_SPEND,
                        COALESCE(m.META_LEADS, 0) AS META_LEADS,
                        d.DAILY_MEMBERSHIPS_SOLD AS MEMBERSHIPS_SOLD,
                        d.DAILY_REVENUE AS MEMBERSHIP_REVENUE,
                        d.CUMULATIVE_MEMBERSHIPS,
                        d.CUMULATIVE_SPEND
                    FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY d
                    LEFT JOIN (
                        SELECT DATE_START, SUM(SPEND) AS AD_SPEND, SUM(LEADS) AS META_LEADS
                        FROM REVRYZE.ANALYTICS.META_ADS
                        WHERE LOCATION_NAME = %s
                          AND CAMPAIGN_NAME IN ({placeholders})
                        GROUP BY DATE_START
                    ) m ON d.DATE = m.DATE_START
                    WHERE d.LOCATION_NAME = %s
                """
                params = [location] + campaign_list + [location]
            else:
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
                date_col = "d.DATE" if campaign_list else "DATE"
                query += f" AND {date_col} >= %s"
                params.append(start_date)
            if end_date:
                date_col = "d.DATE" if campaign_list else "DATE"
                query += f" AND {date_col} <= %s"
                params.append(end_date)
            order_col = "d.DATE" if campaign_list else "DATE"
            query += f" ORDER BY {order_col} ASC"
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


@app.get("/favicon.ico")
def serve_favicon():
    return FileResponse("frontend/favicon_transparent.png", media_type="image/png")


@app.get("/")
def serve_index():
    return FileResponse("frontend/index.html")
