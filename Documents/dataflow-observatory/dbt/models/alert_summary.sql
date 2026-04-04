{{
  config(
    materialized  = 'incremental',
    schema        = 'analytics',
    unique_key    = ['source', 'alert_type', 'field_name', 'severity', 'alert_hour'],
    cluster_by    = ['alert_hour', 'source'],
    on_schema_change = 'append_new_columns'
  )
}}

/*
  alert_summary — Alert counts aggregated per source × alert_type × severity × hour.

  Powers the Grafana "Alert Rate" and "Alert Breakdown" panels.
  The raw quality_alerts table is also the source for the Redis dispatcher,
  so this table gives a durable, queryable version of that stream.
*/

with raw_alerts as (

    select
        alert_id,
        source,
        severity,
        loaded_at,
        payload

    from {{ source('raw', 'quality_alerts') }}

    {% if is_incremental() %}
    where loaded_at > (
        select dateadd('hour', -1, max(alert_hour)) from {{ this }}
    )
    {% endif %}

),

extracted as (

    select
        alert_id,
        source,
        severity,
        loaded_at,

        payload:alert_type::string                          as alert_type,
        payload:field::string                               as field_name,
        payload:message::string                             as message,
        try_to_timestamp_tz(payload:timestamp::string)      as alert_timestamp,
        date_trunc('hour', alert_timestamp)                 as alert_hour,

        -- severity as a numeric score for trend analysis
        case severity
            when 'critical' then 4
            when 'high'     then 3
            when 'medium'   then 2
            else                 1
        end                                                 as severity_score

    from raw_alerts

),

aggregated as (

    select
        alert_hour,
        source,
        alert_type,
        field_name,
        severity,

        count(*)                                            as alert_count,
        round(avg(severity_score), 2)                       as avg_severity_score,

        -- Most common message for this bucket (useful for Grafana annotations)
        mode(message)                                       as top_message,

        max(loaded_at)                                      as last_seen_at

    from extracted
    where alert_hour is not null
    group by 1, 2, 3, 4, 5

)

select * from aggregated
