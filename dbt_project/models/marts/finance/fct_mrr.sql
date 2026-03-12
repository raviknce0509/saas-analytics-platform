-- =============================================================================
-- models/marts/finance/fct_mrr.sql
--
-- PURPOSE: The primary MRR fact table. Powers all revenue dashboards.
--   - Monthly MRR per subscription with movement classification
--   - MRR movements: new, expansion, contraction, churn, reactivation
--   - Joined with account dimension for segmentation
--
-- GRAIN: One row per (subscription_id, month_start)
-- MATERIALIZATION: table — queried constantly by Looker, must be fast
-- USED BY: Finance team, Executive dashboards, Looker MRR explore
-- =============================================================================

with periods as (
    select * from {{ ref('int_subscription_periods') }}
),

accounts as (
    select * from {{ ref('dim_accounts') }}
),

-- ── MRR with Prior Month Comparison ───────────────────────────────────────
-- We use LAG() to look at the previous month's MRR for the same subscription
-- This lets us classify what CHANGED month over month
mrr_with_lag as (

    select
        p.*,

        -- Prior month MRR for this subscription
        lag(p.mrr) over (
            partition by p.subscription_id
            order by p.month_start
        )                                               as prior_month_mrr

    from periods p

),

-- ── MRR Movement Classification ───────────────────────────────────────────
mrr_movements as (

    select
        *,
        -- Delta = how much MRR changed vs last month
        mrr - coalesce(prior_month_mrr, 0)             as mrr_change,

        -- Classify the type of MRR movement
        -- This is the standard SaaS finance framework
        case
            when prior_month_mrr is null or prior_month_mrr = 0
                then 'new'              -- First time we see MRR for this sub
            when mrr > prior_month_mrr
                then 'expansion'        -- Customer upgraded / added seats
            when mrr < prior_month_mrr and mrr > 0
                then 'contraction'      -- Customer downgraded
            when mrr = 0 and prior_month_mrr > 0
                then 'churn'            -- Customer canceled
            when prior_month_mrr = 0 and mrr > 0
                then 'reactivation'     -- Customer came back after churning
            else 'retained'             -- MRR unchanged
        end                                             as mrr_type

    from mrr_with_lag

)

-- ── Final Select: Join with Account Dimension ──────────────────────────────
select
    -- ── Surrogate Key (required for BI tools) ─────────────────────────
    {{ dbt_utils.generate_surrogate_key(['m.subscription_id', 'm.month_start']) }}
                                                        as mrr_id,

    -- ── Dimensions ────────────────────────────────────────────────────
    m.month_start,
    m.subscription_id,
    m.customer_id,
    m.plan_id,
    m.plan_type,
    m.billing_cycle,
    m.subscription_status,

    -- Account dimensions (from dim_accounts — built from Salesforce)
    a.account_id,
    a.account_name,
    a.account_segment,      -- SMB / Mid-Market / Enterprise / Strategic
    a.industry,
    a.billing_country,

    -- ── MRR Metrics ───────────────────────────────────────────────────
    m.mrr,
    m.prior_month_mrr,
    m.mrr_change,
    m.mrr_type,

    -- ── Convenience Flags ─────────────────────────────────────────────
    m.is_first_month,
    m.is_churn_month,

    -- Annualized Run Rate (ARR = MRR × 12) — often requested by Finance
    m.mrr * 12                                          as arr

from mrr_movements m
-- Left join — keep subscriptions even if we can't match an account
-- (data quality issue to flag, not silently drop)
left join accounts a
    on m.customer_id = a.stripe_customer_id
