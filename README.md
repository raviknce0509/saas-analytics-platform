# SaaS Revenue Analytics Platform

> **End-to-end Analytics Engineering project** — dbt + Snowflake + Airflow + Python  
> Built to showcase Analytics Engineering skills for modern data stack roles.

[![dbt CI](https://img.shields.io/badge/dbt-CI%20passing-green)](/.github/workflows/dbt_ci.yml)
[![Python](https://img.shields.io/badge/python-3.11-blue)](requirements.txt)
[![Airflow](https://img.shields.io/badge/airflow-2.9-orange)](airflow/)

---

## What This Project Does

A SaaS company's data was scattered across 4 systems. Finance couldn't reconcile MRR. Growth couldn't see trial conversions. This project builds the single source of truth.

**Sources** → Stripe (billing), Salesforce (CRM), PostgreSQL app DB, Amplitude (events)  
**Output** → Snowflake marts powering Looker dashboards for MRR, NRR, cohort retention, and pipeline velocity

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                              │
│  Stripe API  │  Salesforce API  │  App PostgreSQL  │  Amplitude  │
└──────┬───────┴────────┬─────────┴────────┬──────────┴─────┬─────┘
       │                │                  │                │
       ▼                ▼                  ▼                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Ingestion Layer                                │
│       Fivetran (Stripe/SF)    │    Python ETL (App DB)           │
└──────────────────────────────────────────┬──────────────────────┘
                                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Snowflake RAW Layer                              │
│        STRIPE_RAW  │  SALESFORCE_RAW  │  APP_RAW                 │
└──────────────────────────────────────────┬──────────────────────┘
                                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                  dbt Transformation Layers                        │
│                                                                   │
│  STAGING      →  stg_stripe__*  │  stg_salesforce__*  │  stg_app__*  │
│  INTERMEDIATE →  int_subscription_periods  │  int_trial_conversions    │
│  MARTS        →  fct_mrr  │  fct_subscriptions  │  dim_accounts        │
│  REPORTING    →  rpt_monthly_revenue  │  rpt_cohort_retention           │
└──────────────────────────────────────────┬──────────────────────┘
                                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     BI Layer — Looker                             │
│    MRR Dashboard  │  NRR  │  Cohort Retention  │  Pipeline       │
└─────────────────────────────────────────────────────────────────┘

Orchestration: Apache Airflow (daily 6AM UTC)
CI/CD: GitHub Actions (dbt test on every PR)
```

---

## dbt Model Layers Explained

| Layer | Materialization | Purpose |
|---|---|---|
| **Staging** | View | 1:1 with source table. Rename + type cast only. No business logic. |
| **Intermediate** | View | Complex logic (date spines, window functions, pivots). Not for end users. |
| **Marts** | Table | Business-facing fact and dimension tables. Queried by Looker. |
| **Reporting** | Table | Pre-aggregated for dashboards. No aggregation needed in BI tool. |

---

## Key Models

### `fct_mrr` — The Revenue Engine
- MRR normalized to monthly (annual plans ÷ 12)
- Movement classification: new / expansion / contraction / churn / reactivation
- Joined with account segment (SMB / Mid-Market / Enterprise)

### `int_subscription_periods` — Date Spine
- Explodes subscriptions into one row per (subscription × month)
- Uses `dbt_utils.date_spine` macro
- The building block for all time-series revenue analysis

### `rpt_monthly_revenue` — Executive Dashboard
- Pre-aggregated MRR waterfall by segment
- MoM growth rate via LAG window function
- Direct source for Looker tiles

---

## Local Setup

### Prerequisites
- Python 3.11+
- Docker + Docker Compose
- Snowflake account (free trial works)

### 1. Clone & install
```bash
git clone https://github.com/YOUR_USERNAME/saas-analytics-platform
cd saas-analytics-platform

pip install dbt-core==1.8.0 dbt-snowflake==1.8.0
cd dbt_project && dbt deps
```

### 2. Configure Snowflake
```bash
# Copy sample profile and fill in your credentials
cp dbt_project/profiles.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake account details
```

### 3. Start local services (Airflow + app DB)
```bash
cp .env.example .env   # Fill in your Snowflake creds
docker-compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
# App DB on port 5433 (auto-seeded with test data)
```

### 4. Run dbt
```bash
cd dbt_project
dbt debug          # Test Snowflake connection
dbt seed           # Load seed CSV data
dbt run            # Build all models
dbt test           # Run all data quality tests
dbt docs generate  # Build documentation
dbt docs serve     # View lineage at http://localhost:8080
```

---

## Data Quality

All mart models have 100% test coverage:
- `not_null` + `unique` on all primary keys
- `accepted_values` on status/type columns
- `dbt_expectations` range checks on monetary values
- Custom generic tests: `no_future_dates`, `positive_values`
- Source freshness SLAs enforced in CI

---

## Business Metrics Delivered

| Metric | Model | Consumer |
|---|---|---|
| Monthly MRR by segment | `fct_mrr` | Finance / Exec |
| MRR waterfall (new/expansion/churn) | `rpt_monthly_revenue` | Finance |
| Net Revenue Retention | `rpt_nrr` | Finance / Board |
| Trial → Paid conversion | `rpt_trial_funnel` | Growth |
| Cohort retention curves | `rpt_cohort_retention` | Product / Growth |
| Feature adoption | `rpt_feature_adoption` | Product |

---

## Project Structure

```
saas_analytics/
├── dbt_project/
│   ├── models/
│   │   ├── staging/          # Raw → clean (views)
│   │   │   ├── stripe/
│   │   │   ├── salesforce/
│   │   │   └── app/
│   │   ├── intermediate/     # Complex logic (views)
│   │   ├── marts/            # Business tables
│   │   │   ├── finance/      # fct_mrr, dim_accounts
│   │   │   └── growth/       # fct_trial_conversions
│   │   └── reporting/        # Dashboard-ready tables
│   ├── macros/               # Custom tests + helpers
│   ├── seeds/                # Reference data CSVs
│   └── snapshots/            # SCD Type 2 history
├── airflow/
│   └── dags/
│       └── saas_analytics_daily.py
├── ingestion/
│   ├── extractors/           # Source-specific extract scripts
│   ├── loaders/              # Snowflake load utilities
│   └── validators/           # Schema validation
├── infra/
│   └── sql/                  # Database setup scripts
├── .github/
│   └── workflows/
│       └── dbt_ci.yml        # CI: dbt test on every PR
└── docker-compose.yml
```

---

## Author

Built as a portfolio project demonstrating production-grade Analytics Engineering patterns.

**Skills demonstrated**: dbt Core, Snowflake, Apache Airflow, Python ETL, GitHub Actions CI/CD, data modeling (star schema), SQL window functions, data quality testing, incremental loading patterns, data documentation.
