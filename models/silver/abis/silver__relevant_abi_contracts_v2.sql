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

{% if is_incremental() %},
active_contracts AS (
    SELECT
        contract_address AS created_contract_address
    FROM
        emitted_events
    UNION
    SELECT
        contract_address AS created_contract_address
    FROM
        function_calls
)
{% endif %}
SELECT
    C.created_contract_address AS contract_address,
    COALESCE(
        pe.total_event_count,
        0
    ) + COALESCE(
        e.event_count,
        0
    ) AS total_event_count,
    COALESCE(
        pe.total_call_count,
        0
    ) + COALESCE(
        f.function_call_count,
        0
    ) AS total_call_count,
    COALESCE(
        pe.total_event_count,
        0
    ) + COALESCE(
        e.event_count,
        0
    ) + COALESCE(
        pe.total_call_count,
        0
    ) + COALESCE(
        f.function_call_count,
        0
    ) AS total_interaction_count,
    COALESCE(
        e.max_inserted_timestamp_logs,

{% if is_incremental() %}
pe.max_inserted_timestamp_logs,
{% endif %}

'1970-01-01 00:00:00'
) AS max_inserted_timestamp_logs,
COALESCE(
    f.max_inserted_timestamp_traces,

{% if is_incremental() %}
pe.max_inserted_timestamp_traces,
{% endif %}

'1970-01-01 00:00:00'
) AS max_inserted_timestamp_traces
FROM

{% if is_incremental() %}
active_contracts C
{% else %}
    {{ ref('silver__created_contracts') }} C
{% endif %}
LEFT JOIN emitted_events e
ON C.created_contract_address = e.contract_address
LEFT JOIN function_calls f
ON C.created_contract_address = f.contract_address
LEFT JOIN previous_totals pe
ON C.created_contract_address = pe.contract_address
