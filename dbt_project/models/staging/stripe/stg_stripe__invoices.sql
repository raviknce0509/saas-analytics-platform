-- =============================================================================
-- models/staging/stripe/stg_stripe__invoices.sql
--
-- PURPOSE: Clean Stripe invoices for revenue recognition downstream
-- GRAIN: One row per invoice ID
-- =============================================================================

with source as (
    select * from {{ source('stripe', 'invoices') }}
),

renamed as (

    select
        -- ── IDs ──────────────────────────────────────────────────────
        id                                              as invoice_id,
        subscription                                    as subscription_id,
        customer                                        as customer_id,
        charge                                          as charge_id,
        number                                          as invoice_number,

        -- ── Status ───────────────────────────────────────────────────
        status                                          as invoice_status,
        billing_reason,                                 -- 'subscription_cycle', 'subscription_create', 'manual'

        -- ── Amounts ───────────────────────────────────────────────────
        amount_due / 100.0                              as amount_due_dollars,
        amount_paid / 100.0                             as amount_paid_dollars,
        amount_remaining / 100.0                        as amount_remaining_dollars,
        subtotal / 100.0                                as subtotal_dollars,
        tax / 100.0                                     as tax_dollars,
        total / 100.0                                   as total_dollars,

        -- Currency
        upper(currency)                                 as currency_code,

        -- ── Timestamps ───────────────────────────────────────────────
        to_timestamp(created)::timestamp_ntz            as created_at,
        to_timestamp(period_start)::timestamp_ntz       as period_start_at,
        to_timestamp(period_end)::timestamp_ntz         as period_end_at,
        to_timestamp(due_date)::timestamp_ntz           as due_at,
        to_timestamp(paid_at)::timestamp_ntz            as paid_at,

        -- ── Flags ─────────────────────────────────────────────────────
        paid                                            as is_paid,
        attempted                                       as is_attempted,
        forgiven                                        as is_forgiven,

        _fivetran_synced                                as _fivetran_synced_at

    from source
    where _fivetran_deleted = false

)

select * from renamed
