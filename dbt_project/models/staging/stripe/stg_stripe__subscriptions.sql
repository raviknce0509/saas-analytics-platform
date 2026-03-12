-- =============================================================================
-- models/staging/stripe/stg_stripe__subscriptions.sql
--
-- PURPOSE: Clean and rename the raw Stripe subscriptions table.
--   - Rename columns to snake_case with clear prefixes
--   - Cast all data types explicitly (never trust raw types)
--   - Filter soft-deleted records (_fivetran_deleted)
--   - Extract semi-structured JSON metadata fields
--   - NO business logic here — staging is purely renaming + typing
--
-- MATERIALIZATION: view (always up-to-date, zero storage cost)
-- GRAIN: One row per Stripe subscription ID
-- =============================================================================

with source as (

    -- Reference raw source using source() macro
    -- This enforces lineage tracking in dbt docs
    select * from {{ source('stripe', 'subscriptions') }}

),

renamed as (

    select
        -- ── IDs ──────────────────────────────────────────────────────
        id                                          as subscription_id,
        customer                                    as customer_id,
        plan_id,

        -- ── Status & Type ─────────────────────────────────────────────
        status                                      as subscription_status,

        -- Extract from JSON metadata column (Snowflake VARIANT)
        -- metadata is a VARIANT column — use colon syntax to extract
        metadata:plan_type::varchar                 as plan_type,
        metadata:billing_cycle::varchar             as billing_cycle,
        metadata:acquisition_channel::varchar       as acquisition_channel,

        -- ── Amounts (Stripe stores cents — always convert) ────────────
        plan_amount                                 as plan_amount_cents,
        plan_amount / 100.0                         as plan_amount_dollars,

        -- ── Key Timestamps ────────────────────────────────────────────
        -- Stripe returns Unix timestamps — cast to Snowflake timestamp
        to_timestamp(created)::timestamp_ntz        as created_at,
        to_timestamp(current_period_start)::timestamp_ntz as period_start_at,
        to_timestamp(current_period_end)::timestamp_ntz   as period_end_at,
        to_timestamp(trial_start)::timestamp_ntz    as trial_start_at,
        to_timestamp(trial_end)::timestamp_ntz      as trial_end_at,
        to_timestamp(canceled_at)::timestamp_ntz    as canceled_at,
        to_timestamp(ended_at)::timestamp_ntz       as ended_at,

        -- ── Flags ─────────────────────────────────────────────────────
        cancel_at_period_end                        as is_cancel_at_period_end,

        -- Derived: is this currently in trial?
        case
            when trial_end is not null
             and to_timestamp(trial_end) > current_timestamp
            then true
            else false
        end                                         as is_in_trial,

        -- ── Fivetran metadata ─────────────────────────────────────────
        _fivetran_synced                            as _fivetran_synced_at

    from source

    -- Remove Fivetran soft-deletes (records marked as deleted at source)
    where _fivetran_deleted = false

)

select * from renamed
