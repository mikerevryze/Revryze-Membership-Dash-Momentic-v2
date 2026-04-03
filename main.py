import os
import json
import time

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
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

FALLBACK_LOCATIONS = ["Highland Village", "Lakeview", "West Lake", "Santa Monica"]


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


_cache = {}
_CACHE_TIMEOUT = 600

_sf_account = os.environ.get("SNOWFLAKE_ACCOUNT", "VSC78986.us-east-1")
_sf_user = os.environ.get("SNOWFLAKE_USERNAME", "MIKEPRINCE")
_sf_database = os.environ.get("SNOWFLAKE_DATABASE", "REVRYZE")
_sf_schema = os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS")
_sf_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "DASHBOARD_WH")

def _load_private_key():
    pem_str = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "")
    if not pem_str:
        raise RuntimeError("SNOWFLAKE_PRIVATE_KEY environment variable is not set")
    private_key = serialization.load_pem_private_key(
        pem_str.encode(),
        password=None,
        backend=default_backend(),
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

_private_key_bytes = None
try:
    _private_key_bytes = _load_private_key()
    print("[STARTUP] Private key loaded successfully")
except Exception as e:
    print(f"[STARTUP] WARNING: Failed to load private key: {e}")

print(f"[STARTUP] Snowflake account: {_sf_account}, user: {_sf_user}")


def get_snowflake_conn():
    if _private_key_bytes is None:
        print("[CONN] Snowflake connection failed: private key not loaded")
        return None
    try:
        conn = snowflake.connector.connect(
            account=_sf_account,
            user=_sf_user,
            private_key=_private_key_bytes,
            database=_sf_database,
            schema=_sf_schema,
            warehouse=_sf_warehouse,
            login_timeout=30,
            network_timeout=30,
        )
        return conn
    except Exception as e:
        print(f"[CONN] Snowflake connection failed: {e}")
        return None


@app.get("/api/reset-snowflake")
def reset_snowflake():
    _cache.clear()
    return {"status": "Cache cleared. Next request will query Snowflake directly."}


@app.get("/api/debug")
def debug_snowflake():
    import socket
    try:
        ip = socket.getaddrinfo("VSC78986.us-east-1.snowflakecomputing.com", 443)
        dns_result = str(ip[0])
    except Exception as e:
        dns_result = str(e)

    try:
        sock = socket.create_connection(
            ("VSC78986.us-east-1.snowflakecomputing.com", 443),
            timeout=10
        )
        sock.close()
        tcp_result = "SUCCESS - port 443 reachable"
    except Exception as e:
        tcp_result = f"FAILED - {str(e)}"

    try:
        conn = get_snowflake_conn()
        if conn is None:
            sf_result = "FAILED: private key not loaded or connection error"
        else:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            sf_result = "SUCCESS"
    except Exception as e:
        sf_result = f"FAILED: {type(e).__name__}: {str(e)}"

    return {
        "dns_resolution": dns_result,
        "tcp_connection": tcp_result,
        "snowflake_connection": sf_result,
        "snowflake_account": os.environ.get("SNOWFLAKE_ACCOUNT", "NOT SET"),
        "snowflake_user": os.environ.get("SNOWFLAKE_USERNAME", "NOT SET"),
        "snowflake_warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "NOT SET"),
        "snowflake_private_key_set": bool(os.environ.get("SNOWFLAKE_PRIVATE_KEY")),
    }


@app.on_event("startup")
def warmup_snowflake():
    print("[STARTUP] Attempting synchronous Snowflake connection...")
    try:
        conn = get_snowflake_conn()
        if conn is None:
            raise RuntimeError("Could not establish Snowflake connection")
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        print("[STARTUP] Snowflake connected and warehouse warmed up")

        cur.execute("SELECT LOCATION_NAME FROM REVRYZE.ANALYTICS.LOCATION_MAP ORDER BY LOCATION_NAME")
        locations = [r[0] for r in cur.fetchall()]
        print(f"[STARTUP] Found locations: {locations}")

        for loc in locations:
            try:
                cur.execute("""
                    SELECT
                        COALESCE(SUM(DAILY_SPEND), 0),
                        COALESCE(SUM(DAILY_LEADS), 0),
                        COALESCE(SUM(DAILY_MEMBERSHIPS_SOLD), 0),
                        COALESCE(SUM(DAILY_REVENUE), 0)
                    FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY
                    WHERE LOCATION_NAME = %s
                """, [loc])
                row = cur.fetchone()
                summary_data = {
                    "total_ad_spend": float(row[0]),
                    "total_meta_leads": int(row[1]),
                    "memberships_sold": int(row[2]),
                    "total_membership_revenue": float(row[3]),
                }
                cache_key = f"summary:{loc}:None:None:None"
                _cache[cache_key] = {"data": summary_data, "ts": time.time()}

                cur.execute("""
                    SELECT
                        LOCATION_NAME, DATE AS REPORT_DATE,
                        DAILY_SPEND AS AD_SPEND, DAILY_LEADS AS META_LEADS,
                        DAILY_MEMBERSHIPS_SOLD AS MEMBERSHIPS_SOLD,
                        DAILY_REVENUE AS MEMBERSHIP_REVENUE,
                        CUMULATIVE_MEMBERSHIPS, CUMULATIVE_SPEND
                    FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY
                    WHERE LOCATION_NAME = %s ORDER BY DATE ASC
                """, [loc])
                columns = [desc[0].lower() for desc in cur.description]
                rows = cur.fetchall()
                daily_data = []
                for r in rows:
                    record = {}
                    for i, col in enumerate(columns):
                        val = r[i]
                        if isinstance(val, (date, datetime)):
                            val = val.isoformat()[:10]
                        elif isinstance(val, Decimal):
                            val = float(val)
                        record[col] = val
                    daily_data.append(record)
                daily_cache_key = f"daily:{loc}:None:None:None"
                _cache[daily_cache_key] = {"data": daily_data, "ts": time.time()}

                print(f"[STARTUP] Pre-cached summary and daily for {loc}")
            except Exception as e:
                print(f"[STARTUP] Failed to pre-cache {loc}: {e}")

        conn.close()
        print("[STARTUP] Snowflake startup complete — cache pre-populated")
    except Exception as e:
        print(f"[STARTUP] Snowflake connection failed at startup: {e}")
        print("[STARTUP] App will attempt direct connections per request")


def _auto_add_locations_to_config(location_names):
    """Add newly discovered locations to config.json if they don't already have an entry."""
    cfg = load_config()
    changed = False
    for name in location_names:
        if name not in cfg.get("locations", {}):
            cfg.setdefault("locations", {})[name] = {
                "sales_start_date": None,
                "opening_date": None,
                "selected_campaigns": [],
            }
            changed = True
    if changed:
        save_config(cfg)


@app.get("/api/locations")
def get_locations():
    location_map_locations = set()
    dashboard_locations = set()

    conn = get_snowflake_conn()
    if conn:
        try:
            cur = conn.cursor()
            try:
                cur.execute("SELECT LOCATION_NAME FROM REVRYZE.ANALYTICS.LOCATION_MAP ORDER BY LOCATION_NAME")
                location_map_locations = {r[0] for r in cur.fetchall()}
            except Exception:
                pass
            try:
                cur.execute("SELECT DISTINCT LOCATION_NAME FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY")
                dashboard_locations = {r[0] for r in cur.fetchall()}
            except Exception:
                pass
        except Exception:
            pass
        finally:
            conn.close()

    all_locations = location_map_locations | dashboard_locations
    if all_locations:
        _auto_add_locations_to_config(all_locations)
        return sorted(all_locations)
    return FALLBACK_LOCATIONS


@app.get("/api/campaigns")
def get_campaigns(location: str):
    conn = get_snowflake_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Unable to connect to Snowflake")
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT CAMPAIGN_NAME FROM REVRYZE.ANALYTICS.META_ADS "
            "WHERE LOCATION_NAME = %s AND CAMPAIGN_NAME IS NOT NULL "
            "ORDER BY CAMPAIGN_NAME ASC",
            [location],
        )
        return [r[0] for r in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Snowflake query failed: {e}")
    finally:
        conn.close()


def _parse_campaigns(campaigns_str):
    if not campaigns_str:
        return []
    return [c.strip() for c in campaigns_str.split(",") if c.strip()]


def _query_summary_from_snowflake(location, start_date, end_date, campaign_list):
    conn = get_snowflake_conn()
    if not conn:
        return None
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
        return {
            "total_ad_spend": total_ad_spend,
            "total_meta_leads": total_meta_leads,
            "memberships_sold": memberships_sold,
            "total_membership_revenue": total_membership_revenue,
        }
    except Exception:
        return None
    finally:
        conn.close()


@app.get("/api/summary")
def get_summary(location: str, start_date: str = None, end_date: str = None, campaigns: str = None):
    campaign_list = _parse_campaigns(campaigns)
    cache_key = f"summary:{location}:{start_date}:{end_date}:{campaigns}"

    snowflake_data = _query_summary_from_snowflake(location, start_date, end_date, campaign_list)

    if snowflake_data:
        data_source = "snowflake"
        total_ad_spend = snowflake_data["total_ad_spend"]
        total_meta_leads = snowflake_data["total_meta_leads"]
        memberships_sold = snowflake_data["memberships_sold"]
        total_membership_revenue = snowflake_data["total_membership_revenue"]
        _cache[cache_key] = {"data": snowflake_data, "ts": time.time()}
    elif cache_key in _cache and (time.time() - _cache[cache_key]["ts"]) < _CACHE_TIMEOUT:
        data_source = "cached"
        cached = _cache[cache_key]["data"]
        total_ad_spend = cached["total_ad_spend"]
        total_meta_leads = cached["total_meta_leads"]
        memberships_sold = cached["memberships_sold"]
        total_membership_revenue = cached["total_membership_revenue"]
    else:
        raise HTTPException(status_code=500, detail="Unable to retrieve data from Snowflake")

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
        "data_source": data_source,
    }


def _query_daily_from_snowflake(location, start_date, end_date, campaign_list):
    conn = get_snowflake_conn()
    if not conn:
        return None
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
        return None
    finally:
        conn.close()


@app.get("/api/daily")
def get_daily(location: str, start_date: str = None, end_date: str = None, campaigns: str = None):
    campaign_list = _parse_campaigns(campaigns)
    cache_key = f"daily:{location}:{start_date}:{end_date}:{campaigns}"

    snowflake_data = _query_daily_from_snowflake(location, start_date, end_date, campaign_list)

    if snowflake_data is not None:
        _cache[cache_key] = {"data": snowflake_data, "ts": time.time()}
        return snowflake_data
    elif cache_key in _cache and (time.time() - _cache[cache_key]["ts"]) < _CACHE_TIMEOUT:
        rows = [dict(r) for r in _cache[cache_key]["data"]]
        for r in rows:
            r["data_source"] = "cached"
        return rows
    else:
        raise HTTPException(status_code=500, detail="Unable to retrieve data from Snowflake")


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
