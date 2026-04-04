{{
  config(
    materialized  = 'incremental',
    schema        = 'analytics',
    unique_key    = 'event_id',
    cluster_by    = ['event_hour', 'source'],
    on_schema_change = 'append_new_columns'
  )
}}

/*
  fct_events — Enriched fact table with derived DQ columns and time dimensions.

  Incremental: on each dbt run only new events (by loaded_at) are processed,
  making this safe to run every few minutes alongside the live pipeline.
*/

with staged as (

    select * from {{ ref('stg_events') }}

    {% if is_incremental() %}
    -- Only process events loaded since the last run
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}

),

enriched as (

    select
        -- ── keys & dimensions ─────────────────────────────────────────────────
        event_id,
        source,
        loaded_at,
        event_timestamp,
        date_trunc('hour',  event_timestamp) as event_hour,
        date_trunc('day',   event_timestamp) as event_date,
        dayofweek(event_timestamp)           as day_of_week,
        hour(event_timestamp)                as hour_of_day,

        -- ── clickstream ───────────────────────────────────────────────────────
        event_type,
        user_id,
        session_id,
        url,
        referrer,
        element_id,
        ip_address,
        country,
        duration_ms,
        page_depth,

        -- ── iot_sensor ────────────────────────────────────────────────────────
        sensor_id,
        location,
        temperature,
        humidity,
        battery_level,
        sensor_status,
        firmware,

        -- ── DQ derived columns ────────────────────────────────────────────────
        is_dirty,
        injected_defect,

        -- field-level null flags (used by dq_metrics)
        (user_id  is null) as user_id_is_null,
        (event_type is null) as event_type_is_null,
        (url is null) as url_is_null,
        (sensor_id is null) as sensor_id_is_null,
        (temperature is null) as temperature_is_null,
        (humidity is null) as humidity_is_null,
        (event_timestamp is null) as timestamp_is_null,

        -- range violation flags
        (duration_ms < 0 or duration_ms > 300000) as duration_ms_out_of_range,
        (page_depth  < 0 or page_depth  > 1)      as page_depth_out_of_range,
        (temperature < -50 or temperature > 80)   as temperature_out_of_range,
        (humidity    < 0   or humidity   > 100)   as humidity_out_of_range,
        (battery_level < 0 or battery_level > 100) as battery_out_of_range,

        -- composite DQ score for this single event (1.0 = perfect, 0.0 = all bad)
        case
            when source = 'clickstream' then
                (case when user_id     is not null then 1 else 0 end
               + case when event_type  is not null then 1 else 0 end
               + case when url         is not null then 1 else 0 end
               + case when event_timestamp is not null then 1 else 0 end
               + case when duration_ms between 0 and 300000 then 1 else 0 end
               + case when page_depth  between 0 and 1 then 1 else 0 end) / 6.0
            when source = 'iot_sensor' then
                (case when sensor_id   is not null then 1 else 0 end
               + case when temperature between -50 and 80 then 1 else 0 end
               + case when humidity    between 0 and 100 then 1 else 0 end
               + case when battery_level between 0 and 100 then 1 else 0 end
               + case when event_timestamp is not null then 1 else 0 end) / 5.0
            else null
        end as event_quality_score

    from staged
    where event_id is not null   -- drop rows with no primary key

)

select * from enriched
