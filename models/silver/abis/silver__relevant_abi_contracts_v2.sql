{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    tags = ['abis']
) }}

WITH emitted_events AS (

    SELECT
        contract_address,
        COUNT(*) AS event_count,
        MAX(_inserted_timestamp) AS max_inserted_timestamp_logs
    FROM
        {{ ref('silver__logs') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(max_inserted_timestamp_logs)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    contract_address
),
function_calls AS (
    SELECT
        to_address AS contract_address,
        COUNT(*) AS function_call_count,
        MAX(_inserted_timestamp) AS max_inserted_timestamp_traces
    FROM
        {{ ref('silver__traces') }}
    WHERE
        tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(max_inserted_timestamp_traces)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    to_address
),
active_contracts AS (
    SELECT
        contract_address
    FROM
        emitted_events
    UNION
    SELECT
        contract_address
    FROM
        function_calls
),
previous_totals AS (

{% if is_incremental() %}
SELECT
    contract_address, total_event_count, total_call_count, max_inserted_timestamp_logs, max_inserted_timestamp_traces
FROM
    {{ this }}
{% else %}
SELECT
    NULL AS contract_address, 0 AS total_event_count, 0 AS total_call_count, '1970-01-01 00:00:00' AS max_inserted_timestamp_logs, '1970-01-01 00:00:00' AS max_inserted_timestamp_traces
{% endif %})
SELECT
    C.contract_address,
    COALESCE(
        p.total_event_count,
        0
    ) + COALESCE(
        e.event_count,
        0
    ) AS total_event_count,
    COALESCE(
        p.total_call_count,
        0
    ) + COALESCE(
        f.function_call_count,
        0
    ) AS total_call_count,
    COALESCE(
        p.total_event_count,
        0
    ) + COALESCE(
        e.event_count,
        0
    ) + COALESCE(
        p.total_call_count,
        0
    ) + COALESCE(
        f.function_call_count,
        0
    ) AS total_interaction_count,
    COALESCE(
        e.max_inserted_timestamp_logs,
        p.max_inserted_timestamp_logs,
        '1970-01-01 00:00:00'
    ) AS max_inserted_timestamp_logs,
    COALESCE(
        f.max_inserted_timestamp_traces,
        p.max_inserted_timestamp_traces,
        '1970-01-01 00:00:00'
    ) AS max_inserted_timestamp_traces
FROM
    active_contracts C
    LEFT JOIN emitted_events e
    ON C.contract_address = e.contract_address
    LEFT JOIN function_calls f
    ON C.contract_address = f.contract_address
    LEFT JOIN previous_totals p
    ON C.contract_address = p.contract_address
