-- =============================================================================
-- models/marts/finance/dim_accounts.sql
--
-- PURPOSE: Account dimension table — the "who is our customer" table.
--   Combines Salesforce (CRM data) with app users to create a complete
--   customer profile used across all fact tables.
--
-- GRAIN: One row per customer account (company)
-- MATERIALIZATION: table
-- =============================================================================

with salesforce_accounts as (
    select * from {{ ref('stg_salesforce__accounts') }}
),

-- Get the earliest user signup date per organization
-- This tells us when each company first started using the product
app_users as (
    select
        organization_id,
        min(created_at)                                 as first_user_created_at,
        count(distinct user_id)                         as total_user_count,
        count(distinct case when is_active then user_id end) as active_user_count
    from {{ ref('stg_app__users') }}
    group by 1
),

final as (

    select
        -- ── IDs ──────────────────────────────────────────────────────
        sa.account_id,
        sa.stripe_customer_id,

        -- ── Account Info ──────────────────────────────────────────────
        sa.account_name,
        sa.account_type,
        sa.account_segment,
        sa.industry,
        sa.employee_count,
        sa.annual_revenue_dollars,

        -- ── Geography ─────────────────────────────────────────────────
        sa.billing_country,
        sa.billing_state,

        -- ── User Metrics from App ──────────────────────────────────────
        au.total_user_count,
        au.active_user_count,
        au.first_user_created_at                        as product_first_used_at,

        -- ── CRM Timestamps ────────────────────────────────────────────
        sa.created_at                                   as account_created_at,
        sa.updated_at                                   as account_updated_at,

        -- ── Current Status Flags ──────────────────────────────────────
        case
            when sa.account_type = 'Customer' then true
            else false
        end                                             as is_customer

    from salesforce_accounts sa
    left join app_users au
        -- NOTE: This join assumes organization_id in app maps to account_id in SF
        -- Document this assumption in schema.yml
        on sa.account_id = 'a_' || au.organization_id::varchar

)

select * from final
