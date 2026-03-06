# Revryze Pre-Launch Dashboard

## Overview
Full-stack web dashboard for Revryze (pre-launch membership sales company) that visualizes GHL + Meta Ads data pulled from Snowflake. Used internally to track client performance across multiple locations.

## Stack
- **Backend**: Python 3.11 + FastAPI, queries Snowflake directly
- **Frontend**: React 18 (single page, CDN-loaded) with Recharts for charts
- **Styling**: Tailwind CSS via CDN
- **Config**: `config.json` file for persisted settings (opening dates, sales start dates, attrition)

## File Structure
```
/
├── main.py                  # FastAPI backend with Snowflake integration + sample data fallback
├── config.json              # Persisted settings (auto-created if missing)
├── requirements.txt
├── replit.md
└── frontend/
    └── index.html           # Single-file React app (CDN-loaded React, Recharts, Tailwind)
```

## API Endpoints
- `GET /api/locations` — List of location names from Snowflake or fallback
- `GET /api/summary?location=X&start_date=Y&end_date=Z` — Aggregated KPIs
- `GET /api/daily?location=X&start_date=Y&end_date=Z` — Daily time-series data
- `GET /api/config` — Current config.json
- `POST /api/config` — Overwrite config.json
- `GET /api/reset-snowflake` — Clear Snowflake connection cache to retry

## Environment Variables
- `SNOWFLAKE_ACCOUNT` — Snowflake account identifier
- `SNOWFLAKE_USERNAME` — Snowflake username
- `SNOWFLAKE_PASSWORD` — Snowflake password (secret)
- `SNOWFLAKE_DATABASE` — Database name (REVRYZE)
- `SNOWFLAKE_SCHEMA` — Schema name (ANALYTICS)
- `SNOWFLAKE_WAREHOUSE` — Warehouse name (INGEST_WH)

## Snowflake Tables
- `REVRYZE.ANALYTICS.LOCATION_MAP` — Location names
- `REVRYZE.ANALYTICS.DASHBOARD_DAILY` — Daily metrics per location (AD_SPEND, META_LEADS, MEMBERSHIPS_SOLD, MEMBERSHIP_REVENUE, REPORT_DATE, LOCATION_NAME)

## Key Features
- Location tabs that dynamically load from Snowflake's LOCATION_MAP
- Date range filtering (All Time, Last 7/30/60 days, Custom)
- 6 KPI cards: Ad Spend, Meta Leads, CPL, Memberships Sold, Revenue, Cost/Membership
- Two line charts: Daily Ad Spend + Cumulative Memberships with projection
- Sales Velocity section with projected membership count by opening day
- Collapsible Settings panel with attrition model + LTV calculation
- Graceful fallback to sample data when Snowflake is unavailable

## Running
```
uvicorn main:app --host 0.0.0.0 --port 5000
```
