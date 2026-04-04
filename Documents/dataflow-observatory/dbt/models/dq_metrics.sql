{{
  config(
    materialized  = 'incremental',
    schema        = 'analytics',
    unique_key    = ['source', 'event_hour'],
    cluster_by    = ['event_hour', 'source'],
    on_schema_change = 'append_new_columns'
  )
}}

/*
  dq_metrics — Data quality scores aggregated per source per hour.

  This is the primary table powering the Grafana "DQ Score over Time" panel.
  Each row = one source × one hour → quality_score in [0, 1].
*/

with events as (

    select * from {{ ref('fct_events') }}

    {% if is_incremental() %}
    where event_hour > (
        select dateadd('hour', -1, max(event_hour)) from {{ this }}
    )
    -- Reprocess the last complete hour in case late arrivals changed the score
    {% endif %}

),

aggregated as (

    select
        event_hour,
        source,

        -- ── volume ────────────────────────────────────────────────────────────
        count(*)                                        as total_events,
        count_if(is_dirty)                               as dirty_events,
        count_if(not is_dirty)                           as clean_events,

        -- ── null rates per critical field ─────────────────────────────────────
        avg(case when source = 'clickstream'
                 then user_id_is_null::int end)         as null_rate_user_id,
        avg(case when source = 'clickstream'
                 then event_type_is_null::int end)      as null_rate_event_type,
        avg(case when source = 'clickstream'
                 then url_is_null::int end)             as null_rate_url,
        avg(case when source = 'iot_sensor'
                 then sensor_id_is_null::int end)       as null_rate_sensor_id,
        avg(case when source = 'iot_sensor'
                 then temperature_is_null::int end)     as null_rate_temperature,
        avg(case when source = 'iot_sensor'
                 then humidity_is_null::int end)        as null_rate_humidity,
        avg(timestamp_is_null::int)                     as null_rate_timestamp,

        -- ── range violations ─────────────────────────────────────────────────
        count_if(duration_ms_out_of_range)               as duration_ms_violations,
        count_if(page_depth_out_of_range)                as page_depth_violations,
        count_if(temperature_out_of_range)               as temperature_violations,
        count_if(humidity_out_of_range)                  as humidity_violations,
        count_if(battery_out_of_range)                   as battery_violations,

        -- ── composite quality score [0, 1] ────────────────────────────────────
        -- Average of per-event scores; null events excluded from avg automatically
        round(avg(event_quality_score), 4)              as quality_score,

        -- ── latency proxy: avg gap between event_timestamp and loaded_at ──────
        round(avg(
            datediff('second', event_timestamp, loaded_at)
        ), 1)                                           as avg_ingest_lag_seconds,

        max(loaded_at)                                  as last_loaded_at

    from events
    where event_hour is not null
    group by 1, 2

)

select * from aggregated
