# Revryze Pre-Launch Dashboard

## Overview
Full-stack web dashboard for Revryze (pre-launch membership sales company) that visualizes GHL + Meta Ads data pulled from Snowflake, with Google Sheets CDL integration and a Goal Planner. Used internally to track client performance across multiple locations.

## Stack
- **Backend**: Python 3.11 + FastAPI, queries Snowflake (key-pair auth) and Google Sheets (service account)
- **Frontend**: React 18 (single page, CDN-loaded) with Recharts for charts
- **Styling**: Tailwind CSS via CDN
- **Config**: `config.json` file for persisted settings (opening dates, sales start dates, attrition, sheet IDs)

## File Structure
```
/
├── main.py                  # FastAPI backend with Snowflake + Google Sheets integration
├── config.json              # Persisted settings (locations, sheet IDs, attrition)
├── requirements.txt         # fastapi, uvicorn, snowflake-connector-python, cryptography, google-auth, gspread
├── rsa_key_pkcs8.pem        # Snowflake key-pair auth private key (PKCS8 format)
├── replit.md
└── frontend/
    ├── index.html           # Single-file React app (CDN-loaded React, Recharts, Tailwind)
    ├── logo.png
    └── favicon_transparent.png
```

## API Endpoints
- `GET /api/locations` — List of location names from Snowflake
- `GET /api/campaigns?location=X` — Distinct campaign names from META_ADS for a location
- `GET /api/summary?location=X&start_date=Y&end_date=Z&campaigns=A,B` — Aggregated KPIs
- `GET /api/daily?location=X&start_date=Y&end_date=Z&campaigns=A,B` — Daily time-series
- `GET /api/cdls?location=X` — CDL count from Google Sheets for a location
- `GET /api/config` / `POST /api/config` — Read/write config.json
- `GET /api/reset-snowflake` — Clear cache
- `GET /api/debug` — Snowflake connection diagnostics

## Environment Secrets
- `SNOWFLAKE_PRIVATE_KEY` — PEM private key for Snowflake key-pair auth (PKCS8 format, raw base64 OK)
- `GOOGLE_SERVICE_ACCOUNT_JSON` — Full Google service account JSON credentials for Sheets API access
- `SNOWFLAKE_ACCOUNT` — defaults to VSC78986.us-east-1
- `SNOWFLAKE_USERNAME` — defaults to MIKEPRINCE
- `SNOWFLAKE_DATABASE` — defaults to REVRYZE
- `SNOWFLAKE_SCHEMA` — defaults to ANALYTICS
- `SNOWFLAKE_WAREHOUSE` — defaults to DASHBOARD_WH

## Snowflake Tables
- `REVRYZE.ANALYTICS.LOCATION_MAP` — Columns: LOCATION_NAME, AD_ACCOUNT_ID
- `REVRYZE.ANALYTICS.DASHBOARD_DAILY` — Columns: LOCATION_NAME, DATE, DAILY_SPEND, DAILY_LEADS, DAILY_MEMBERSHIPS_SOLD, DAILY_REVENUE, CUMULATIVE_MEMBERSHIPS, CUMULATIVE_SPEND
- `REVRYZE.ANALYTICS.META_ADS` — Columns: LOCATION_NAME, DATE_START, CAMPAIGN_NAME, SPEND, LEADS, IMPRESSIONS, CLICKS, etc.

## Google Sheets CDL Integration
Each location has a Google Form response sheet. The service account reads non-empty rows (excluding header) to get CDL counts. Sheet IDs and GIDs are stored in config.json per location:
- Highland Village: `1gjMjw6Vre5SKKThQm04lYbfeP3hASAv-4a8P7oIu4fc` (gid: 2140979712)
- Lakeview: `1GQI62TCY8ZFb8Uh-fJTcOkpMGb2WV5oehAsS5cUfT3U` (gid: 2049338531)
- Santa Monica: `1T1ZDBzu3I9ARFcQ2ic2S2JCoNVEXLxOuDbuFqL7Od14` (gid: 1661266551)
- West Lake: `17PPhkPaf7HkRWG_HpZpkqqXWgtatov_c1yiHh4_9-i8` (gid: 1992602451)

## Key Features
- Location tabs dynamically loaded from Snowflake LOCATION_MAP
- Date range filtering (All Time, Last 7/30/60 days, Custom)
- 8 KPI cards: Ad Spend, Meta Leads, Community Driven Leads (CDL), CPL, Memberships Sold, Revenue, Cost/Membership, Avg Membership Value
- LTV callout with configurable attrition model
- Two line charts: Daily Ad Spend + Cumulative Memberships with projection
- Sales Velocity section with projected membership count by opening day
- Goal Planner section: progress bars (CDL vs 500 goal, members vs 250 goal), blended close rate, projected CDL slider with dynamic ad spend calculation
- Per-location campaign filter (Settings panel)
- 10-minute cache with startup pre-population
- No fallback/sample data — returns HTTP 500 if Snowflake unavailable; frontend retries every 5s

## Snowflake Auth
Uses key-pair authentication (PKCS8 private key → DER bytes). The `SNOWFLAKE_PRIVATE_KEY` secret handles raw base64 (no headers/newlines) by reconstructing PEM format automatically.

## Running
```
uvicorn main:app --host 0.0.0.0 --port 5000
```

## CDN Dependencies
- React 18 (production min) + ReactDOM
- PropTypes (required by Recharts UMD)
- Babel standalone (in-browser JSX transpilation)
- Recharts 2.5.0 (NOT 2.12.x — UMD export issues)
- Tailwind CSS
