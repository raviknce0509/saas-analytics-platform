-- =============================================================================
-- models/staging/app/stg_app__users.sql
--
-- PURPOSE: Clean app user records from production PostgreSQL DB
-- GRAIN: One row per user (unique user_id)
-- =============================================================================

with source as (
    select * from {{ source('app_db', 'users') }}
),

renamed as (

    select
        -- ── IDs ──────────────────────────────────────────────────────
        id                                              as user_id,
        -- Stripe customer ID is attached at signup when they add a card
        stripe_customer_id                              as stripe_customer_id,
        organization_id,

        -- ── User Info ─────────────────────────────────────────────────
        email,
        -- Never store PII in marts — hash email for joins, keep raw in staging only
        md5(lower(trim(email)))                         as email_hash,
        first_name,
        last_name,
        first_name || ' ' || last_name                  as full_name,

        -- ── Role & Status ─────────────────────────────────────────────
        role                                            as user_role,        -- 'admin', 'member', 'viewer'
        status                                          as user_status,      -- 'active', 'invited', 'deactivated'

        -- ── Signup Context ────────────────────────────────────────────
        signup_source,          -- 'organic', 'paid_search', 'referral', 'product_led'
        referral_code,

        -- ── Timestamps ───────────────────────────────────────────────
        created_at::timestamp_ntz                       as created_at,
        updated_at::timestamp_ntz                       as updated_at,
        last_login_at::timestamp_ntz                    as last_login_at,
        email_verified_at::timestamp_ntz                as email_verified_at,

        -- ── Derived Flags ─────────────────────────────────────────────
        case when email_verified_at is not null then true else false end  as is_email_verified,
        case when status = 'active'             then true else false end  as is_active,

        -- Ingestion metadata (added by our Python ETL script)
        _loaded_at

    from source

)

select * from renamed
