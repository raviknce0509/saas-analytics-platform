-- =============================================================================
-- models/intermediate/int_subscription_periods.sql
--
-- PURPOSE: The heart of MRR calculation.
--   Explodes subscriptions into monthly periods using a date spine.
--   Each row = one subscription active in one calendar month.
--
-- CONCEPT (critical to understand for interviews):
--   A date spine is a table of every date/month you care about.
--   By joining subscriptions to months where they were active,
--   we get the granularity needed for MRR time-series analysis.
--
-- GRAIN: One row per (subscription_id, month)
-- =============================================================================

with subscriptions as (
    select * from {{ ref('stg_stripe__subscriptions') }}
),

-- ── Date Spine ────────────────────────────────────────────────────────────
-- Generates a row for every month from 2022-01 to current month + 1
-- dbt_utils.date_spine creates this sequence automatically
date_spine as (

    {{
        dbt_utils.date_spine(
            datepart    = "month",
            start_date  = "cast('2022-01-01' as date)",
            end_date    = "dateadd(month, 1, date_trunc('month', current_date))"
        )
    }}

),

-- Clean up the date spine column name
months as (
    select
        date_trunc('month', date_month)::date as month_start
    from date_spine
),

-- ── Core Join: Subscriptions × Months ─────────────────────────────────────
-- A subscription is "active" in a month if:
--   period_start_at <= month_start  AND
--   (canceled_at IS NULL OR canceled_at > month_start)
subscription_months as (

    select
        -- ── Keys ──────────────────────────────────────────────────────
        s.subscription_id,
        s.customer_id,
        s.plan_id,
        m.month_start,

        -- ── Plan Info ─────────────────────────────────────────────────
        s.plan_type,
        s.billing_cycle,
        s.subscription_status,

        -- ── MRR Calculation ───────────────────────────────────────────
        -- Normalize to Monthly Recurring Revenue regardless of billing cycle
        -- Annual plan: divide annual amount by 12
        -- Monthly plan: use amount directly
        case
            when s.billing_cycle = 'annual'
                then s.plan_amount_dollars / 12.0
            when s.billing_cycle = 'monthly'
                then s.plan_amount_dollars
            -- Fallback: assume monthly
            else s.plan_amount_dollars
        end                                             as mrr,

        -- ── Subscription lifecycle flags per month ────────────────────
        -- Is this the first month of the subscription?
        case
            when date_trunc('month', s.period_start_at) = m.month_start
            then true else false
        end                                             as is_first_month,

        -- Is this the churn month?
        case
            when s.canceled_at is not null
             and date_trunc('month', s.canceled_at) = m.month_start
            then true else false
        end                                             as is_churn_month,

        -- ── Raw timestamps (useful for debugging) ─────────────────────
        s.period_start_at,
        s.canceled_at,
        s.trial_start_at,
        s.trial_end_at

    from subscriptions s
    inner join months m
        -- Only include months where the subscription was active
        on  m.month_start >= date_trunc('month', s.period_start_at)
        and m.month_start <  coalesce(
                                date_trunc('month', s.canceled_at),
                                '2099-01-01'::date  -- open-ended subscriptions
                              )

    -- Exclude zero-value subscriptions (freemium/test accounts)
    where s.plan_amount_dollars > 0

)

select * from subscription_months
