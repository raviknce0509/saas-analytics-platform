-- =============================================================================
-- models/reporting/rpt_monthly_revenue.sql
--
-- PURPOSE: Executive-level monthly revenue summary.
--   Pre-aggregated for Looker dashboards — no aggregation needed in BI tool.
--   This is what Finance reviews every month-end.
--
-- CONSUMERS: Finance team, Executive dashboard, Board reporting
-- GRAIN: One row per (month_start, account_segment)
-- =============================================================================

with mrr as (
    select * from {{ ref('fct_mrr') }}
),

monthly_by_segment as (

    select
        month_start,
        account_segment,

        -- ── Total MRR ─────────────────────────────────────────────────
        sum(mrr)                                        as total_mrr,
        sum(mrr) * 12                                   as total_arr,

        -- ── MRR by Movement Type ──────────────────────────────────────
        -- These are the building blocks of the MRR waterfall chart
        sum(case when mrr_type = 'new'         then mrr else 0 end) as new_mrr,
        sum(case when mrr_type = 'expansion'   then mrr_change else 0 end) as expansion_mrr,
        sum(case when mrr_type = 'contraction' then mrr_change else 0 end) as contraction_mrr,
        sum(case when mrr_type = 'churn'       then prior_month_mrr else 0 end) as churned_mrr,
        sum(case when mrr_type = 'reactivation' then mrr else 0 end) as reactivation_mrr,

        -- ── Account Counts ─────────────────────────────────────────────
        count(distinct customer_id)                     as active_customer_count,
        count(distinct case when mrr_type = 'new'   then customer_id end) as new_customer_count,
        count(distinct case when mrr_type = 'churn' then customer_id end) as churned_customer_count,

        -- ── Derived Rates ──────────────────────────────────────────────
        -- Churn Rate = Churned MRR / Beginning MRR
        -- Computed in Looker via table calculations, not here
        -- (avoids division by zero edge cases in SQL)

        -- Average MRR per customer (ARPU)
        sum(mrr) / nullif(count(distinct customer_id), 0) as arpu

    from mrr
    group by 1, 2

),

-- Add Month-over-Month growth via LAG
with_mom_growth as (

    select
        *,

        lag(total_mrr) over (
            partition by account_segment
            order by month_start
        )                                               as prior_month_mrr,

        -- MoM growth rate
        (total_mrr - lag(total_mrr) over (
            partition by account_segment order by month_start
        )) / nullif(lag(total_mrr) over (
            partition by account_segment order by month_start
        ), 0)                                           as mom_growth_rate

    from monthly_by_segment

)

select * from with_mom_growth
order by month_start desc, account_segment
