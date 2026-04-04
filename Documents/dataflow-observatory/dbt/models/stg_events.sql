{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}

/*
  stg_events — Extracts and type-casts all fields from the raw VARIANT payload.

  One row per raw event. Dirty events are kept (is_dirty = true) so downstream
  models can measure quality without losing the original data.
*/

with source as (

    select
        event_id,
        source,
        loaded_at,
        payload

    from {{ source('raw', 'events') }}

),

extracted as (

    select
        -- ── metadata ──────────────────────────────────────────────────────────
        event_id,
        source,
        loaded_at,

        -- timestamp may be a valid ISO string or dirty (int / malformed string)
        try_to_timestamp_tz(payload:timestamp::string) as event_timestamp,
        payload:timestamp::string                      as raw_timestamp,

        -- ── clickstream fields ────────────────────────────────────────────────
        payload:event_type::string                     as event_type,
        payload:user_id::string                        as user_id,
        payload:session_id::string                     as session_id,
        payload:url::string                            as url,
        payload:referrer::string                       as referrer,
        payload:element_id::string                     as element_id,
        payload:ip_address::string                     as ip_address,
        payload:country::string                        as country,
        try_to_number(payload:duration_ms::string)      as duration_ms,
        try_to_double(payload:page_depth::string)      as page_depth,

        -- ── iot_sensor fields ─────────────────────────────────────────────────
        payload:sensor_id::string                      as sensor_id,
        payload:location::string                       as location,
        try_to_double(payload:temperature::string)     as temperature,
        try_to_double(payload:humidity::string)        as humidity,
        try_to_double(payload:battery_level::string)   as battery_level,
        payload:status::string                         as sensor_status,
        payload:firmware::string                       as firmware,

        -- ── DQ flags ─────────────────────────────────────────────────────────
        payload:_defect::string                        as injected_defect,
        (payload:_defect is not null)                  as is_dirty

    from source

)

select * from extracted
