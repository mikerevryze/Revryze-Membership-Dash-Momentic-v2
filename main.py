import os
import json
from datetime import date, datetime
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
        "Highland Village": {"sales_start_date": None, "opening_date": None},
        "Lakeview": {"sales_start_date": None, "opening_date": None},
    },
    "attrition": {"mode": "attrition_rate", "attrition_rate": 5.0, "avg_monthly_stay": 12},
}


def load_config():
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def get_snowflake_conn():
    try:
        return snowflake.connector.connect(
            account=os.environ.get("SNOWFLAKE_ACCOUNT", "VSC78986.us-east-1"),
            user=os.environ.get("SNOWFLAKE_USERNAME", "MIKEPRINCE"),
            password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
            database=os.environ.get("SNOWFLAKE_DATABASE", "REVRYZE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "INGEST_WH"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Snowflake connection failed: {str(e)}")


@app.get("/api/locations")
def get_locations():
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT LOCATION_NAME FROM REVRYZE.ANALYTICS.LOCATION_MAP ORDER BY LOCATION_NAME")
        rows = cur.fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        conn.close()


@app.get("/api/summary")
def get_summary(location: str, start_date: str = None, end_date: str = None):
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()

        query = """
            SELECT
                COALESCE(SUM(AD_SPEND), 0) AS total_ad_spend,
                COALESCE(SUM(META_LEADS), 0) AS total_meta_leads,
                COALESCE(SUM(MEMBERSHIPS_SOLD), 0) AS memberships_sold,
                COALESCE(SUM(MEMBERSHIP_REVENUE), 0) AS total_membership_revenue
            FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY
            WHERE LOCATION_NAME = %s
        """
        params = [location]

        if start_date:
            query += " AND REPORT_DATE >= %s"
            params.append(start_date)
        if end_date:
            query += " AND REPORT_DATE <= %s"
            params.append(end_date)

        cur.execute(query, params)
        row = cur.fetchone()

        total_ad_spend = float(row[0])
        total_meta_leads = int(row[1])
        memberships_sold = int(row[2])
        total_membership_revenue = float(row[3])

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
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        conn.close()


@app.get("/api/daily")
def get_daily(location: str, start_date: str = None, end_date: str = None):
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        query = "SELECT * FROM REVRYZE.ANALYTICS.DASHBOARD_DAILY WHERE LOCATION_NAME = %s"
        params = [location]

        if start_date:
            query += " AND REPORT_DATE >= %s"
            params.append(start_date)
        if end_date:
            query += " AND REPORT_DATE <= %s"
            params.append(end_date)

        query += " ORDER BY REPORT_DATE ASC"
        cur.execute(query, params)

        columns = [desc[0].lower() for desc in cur.description]
        rows = cur.fetchall()

        result = []
        for row in rows:
            record = {}
            for i, col in enumerate(columns):
                val = row[i]
                if isinstance(val, (date, datetime)):
                    val = val.isoformat()
                elif isinstance(val, bytes):
                    val = val.decode("utf-8")
                record[col] = val
            result.append(record)

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        conn.close()


@app.get("/api/config")
def get_config():
    return load_config()


@app.post("/api/config")
async def post_config(request: Request):
    body = await request.json()
    save_config(body)
    return body


app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")


@app.get("/")
def serve_index():
    return FileResponse("frontend/index.html")
