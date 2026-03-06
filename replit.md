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
- `GET /api/campaigns?location=X` — Distinct campaign names from META_ADS for a location
- `GET /api/summary?location=X&start_date=Y&end_date=Z&campaigns=A,B` — Aggregated KPIs (campaigns filter uses META_ADS for spend/leads, DASHBOARD_DAILY for memberships/revenue)
- `GET /api/daily?location=X&start_date=Y&end_date=Z&campaigns=A,B` — Daily time-series (campaigns filter joins META_ADS spend/leads with DASHBOARD_DAILY memberships/revenue)
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
- `REVRYZE.ANALYTICS.LOCATION_MAP` — Columns: LOCATION_NAME, AD_ACCOUNT_ID
- `REVRYZE.ANALYTICS.DASHBOARD_DAILY` — Columns: LOCATION_NAME, DATE (timestamp), DAILY_SPEND (float), DAILY_LEADS (int), DAILY_MEMBERSHIPS_SOLD (int), DAILY_REVENUE (decimal), CUMULATIVE_MEMBERSHIPS (int), CUMULATIVE_SPEND (float)
  - Queries alias these to: report_date, ad_spend, meta_leads, memberships_sold, membership_revenue (for frontend compatibility)
- `REVRYZE.ANALYTICS.META_ADS` — Columns: LOCATION_NAME, DATE_START, CAMPAIGN_NAME, SPEND, LEADS, IMPRESSIONS, CLICKS, CPM, CPC, CTR, ADSET_NAME, AD_NAME
  - Used for campaign-filtered queries (spend/leads per campaign)

## Key Features
- Location tabs that dynamically load from Snowflake's LOCATION_MAP
- Date range filtering (All Time, Last 7/30/60 days, Custom)
- 7 KPI cards: Ad Spend, Meta Leads, CPL, Memberships Sold, Revenue, Cost/Membership, Avg Membership Value
- LTV callout with configurable attrition model (attrition rate or avg monthly stay)
- Two line charts: Daily Ad Spend + Cumulative Memberships with projection trajectory
- Sales Velocity section with projected membership count by opening day
- Per-location campaign filter (checkboxes in Settings panel) — filters ad spend/leads from META_ADS table; memberships/revenue always from DASHBOARD_DAILY (GHL data isn't campaign-specific)
- Collapsible Settings panel for location dates and campaign selection
- Graceful fallback to sample data when Snowflake is unavailable

## Snowflake Connection Strategy
- Snowflake probe runs in a background subprocess with a 30-second delay to avoid CPU competition with initial page load
- `_snowflake_available` defaults to False; API immediately serves fallback data
- If Snowflake connects successfully after 30s, subsequent requests use real data
- No GIL blocking: probe uses subprocess.run() in a daemon thread, not snowflake.connector in-process

## Running
```
uvicorn main:app --host 0.0.0.0 --port 5000
```

## CDN Dependencies
- React 18 (production min) + ReactDOM
- PropTypes (required by Recharts UMD)
- Babel standalone (in-browser JSX transpilation)
- Recharts 2.5.0 (NOT 2.12.x — that version has UMD export issues)
- Tailwind CSS
