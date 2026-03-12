-- =============================================================================
-- models/staging/salesforce/stg_salesforce__accounts.sql
--
-- PURPOSE: Clean Salesforce Account records
-- GRAIN: One row per Salesforce Account (= one customer company)
-- NOTE: This joins with Stripe via customer_id in downstream models
-- =============================================================================

with source as (
    select * from {{ source('salesforce', 'account') }}
),

renamed as (

    select
        -- ── IDs ──────────────────────────────────────────────────────
        id                                              as account_id,
        -- Stripe customer ID stored as custom field in Salesforce
        stripe_customer_id__c                           as stripe_customer_id,

        -- ── Account Info ──────────────────────────────────────────────
        name                                            as account_name,
        type                                            as account_type,     -- 'Customer', 'Partner', 'Prospect'
        industry,
        numberofemployees                               as employee_count,
        annualrevenue                                   as annual_revenue_dollars,

        -- ── Segmentation ──────────────────────────────────────────────
        -- Map employee count to segment (critical for finance reporting)
        case
            when numberofemployees < 10   then 'SMB'
            when numberofemployees < 200  then 'Mid-Market'
            when numberofemployees < 1000 then 'Enterprise'
            else                               'Strategic'
        end                                             as account_segment,

        -- ── Geography ─────────────────────────────────────────────────
        billingcountry                                  as billing_country,
        billingstate                                    as billing_state,

        -- ── Owner ─────────────────────────────────────────────────────
        ownerid                                         as account_owner_id,

        -- ── Timestamps ───────────────────────────────────────────────
        createddate::timestamp_ntz                      as created_at,
        lastmodifieddate::timestamp_ntz                 as updated_at,

        -- ── Fivetran ──────────────────────────────────────────────────
        _fivetran_synced                                as _fivetran_synced_at,
        isdeleted

    from source
    where isdeleted = false

)

select * from renamed
