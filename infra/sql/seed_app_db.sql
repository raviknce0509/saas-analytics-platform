-- =============================================================================
-- infra/sql/seed_app_db.sql
--
-- PURPOSE: Populate local PostgreSQL with realistic test data.
--   Run automatically by Docker when container starts.
--   This lets you develop and test WITHOUT needing Stripe/Salesforce APIs.
-- =============================================================================

-- ── Schema Setup ──────────────────────────────────────────────────────────────
create schema if not exists public;

-- ── Users Table ───────────────────────────────────────────────────────────────
create table if not exists users (
    id                  serial primary key,
    email               varchar(255) unique not null,
    first_name          varchar(100),
    last_name           varchar(100),
    organization_id     integer,
    stripe_customer_id  varchar(50),
    role                varchar(50)  default 'member',
    status              varchar(50)  default 'active',
    signup_source       varchar(100),
    referral_code       varchar(50),
    created_at          timestamp    default now(),
    updated_at          timestamp    default now(),
    last_login_at       timestamp,
    email_verified_at   timestamp
);

-- ── Subscriptions Table ───────────────────────────────────────────────────────
create table if not exists subscriptions (
    id                  serial primary key,
    user_id             integer references users(id),
    organization_id     integer,
    stripe_sub_id       varchar(50),
    plan_name           varchar(100),
    plan_type           varchar(50),  -- 'monthly', 'annual'
    status              varchar(50),
    amount_cents        integer,
    started_at          timestamp,
    canceled_at         timestamp,
    updated_at          timestamp default now()
);

-- ── Events Table ──────────────────────────────────────────────────────────────
create table if not exists events (
    event_id            uuid primary key default gen_random_uuid(),
    user_id             integer references users(id),
    session_id          uuid,
    event_name          varchar(200) not null,
    event_properties    jsonb,
    occurred_at         timestamp not null default now()
);

-- ── Sessions Table ────────────────────────────────────────────────────────────
create table if not exists sessions (
    session_id          uuid primary key default gen_random_uuid(),
    user_id             integer references users(id),
    started_at          timestamp,
    ended_at            timestamp,
    page_views          integer,
    events_count        integer
);

-- ── Seed Data ─────────────────────────────────────────────────────────────────
-- Generate 200 realistic users across 50 organizations

insert into users (email, first_name, last_name, organization_id, stripe_customer_id,
                   role, status, signup_source, created_at, updated_at,
                   last_login_at, email_verified_at)
select
    'user_' || i || '@company_' || ceil(i::float/4) || '.com',
    (array['Alice','Bob','Carol','David','Eva','Frank','Grace','Henry'])[mod(i,8)+1],
    (array['Smith','Jones','Williams','Brown','Davis','Miller','Wilson','Moore'])[mod(i,8)+1],
    ceil(i::float/4)::int,
    'cus_' || md5(i::text),
    case mod(i,5) when 0 then 'admin' else 'member' end,
    case mod(i,20) when 0 then 'deactivated' else 'active' end,
    (array['organic','paid_search','referral','product_led'])[mod(i,4)+1],
    now() - (random() * 730) * interval '1 day',
    now() - (random() * 30) * interval '1 day',
    now() - (random() * 7) * interval '1 day',
    now() - (random() * 700) * interval '1 day'
from generate_series(1, 200) as i;

-- Generate subscriptions (one per organization)
insert into subscriptions (user_id, organization_id, stripe_sub_id, plan_name,
                            plan_type, status, amount_cents, started_at)
select
    (ceil(i::float * 4))::int as user_id,
    i as organization_id,
    'sub_' || md5(i::text),
    (array['Starter','Growth','Business','Enterprise'])[mod(i,4)+1],
    case mod(i,3) when 0 then 'annual' else 'monthly' end,
    case mod(i,15) when 0 then 'canceled' else 'active' end,
    (array[4900, 9900, 29900, 99900])[mod(i,4)+1],
    now() - (random() * 600) * interval '1 day'
from generate_series(1, 50) as i;

-- Generate events (50 per user = 10,000 events)
insert into events (user_id, session_id, event_name, event_properties, occurred_at)
select
    (mod(i, 200) + 1)::int,
    gen_random_uuid(),
    (array[
        'page_view', 'feature_click', 'export_data',
        'invite_member', 'create_project', 'dashboard_view',
        'api_call', 'settings_update', 'report_generated'
    ])[mod(i,9)+1],
    ('{"source": "web", "feature": "' ||
      (array['analytics','reports','integrations','settings'])[mod(i,4)+1] ||
     '"}')::jsonb,
    now() - (random() * 90) * interval '1 day'
from generate_series(1, 10000) as i;

-- ── Indexes (critical for ETL performance) ────────────────────────────────────
create index if not exists idx_users_updated_at       on users(updated_at);
create index if not exists idx_subscriptions_updated  on subscriptions(updated_at);
create index if not exists idx_events_occurred_at     on events(occurred_at);
create index if not exists idx_events_user_id         on events(user_id);

-- Confirm seed
select 'users' as tbl, count(*) from users
union all
select 'subscriptions', count(*) from subscriptions
union all
select 'events', count(*) from events;
