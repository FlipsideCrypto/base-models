{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)"
) }}

WITH logs_response AS (

SELECT
	block_number,
    blockHash AS block_hash,
    logs_array,
	VALUE :address :: STRING AS contract_address,
    VALUE :data :: STRING AS data,
    ethereum.public.udf_hex_to_int(
    	VALUE :logIndex :: STRING) :: INTEGER AS event_index,
    VALUE :removed :: STRING AS removed,
    VALUE :topics AS topics,
    VALUE :transactionHash :: STRING AS tx_hash,
    VALUE :transactionIndex :: STRING AS transactionIndex,
    origin_from_address,
    origin_to_address,
    status,
    type,
    CONCAT(
        tx_hash,
        '-',
        event_index
    ) AS _log_id,
    _inserted_timestamp
FROM {{ ref('silver_goerli__receipts_method') }},
    LATERAL FLATTEN(input => logs_array)
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),

flat_base AS (

    SELECT
        _log_id,
        block_number,
        block_hash,
        tx_hash,
        origin_from_address,
        CASE
            WHEN len(origin_to_address) <= 0 THEN NULL
            ELSE origin_to_address
        END AS origin_to_address,
        event_index,
        contract_address,
        topics,
        DATA,
        CASE
            WHEN removed = 'true' THEN TRUE
            ELSE FALSE
        END AS event_removed,
        CASE
            WHEN status = '0x1' THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        ethereum.public.udf_hex_to_int(
            transactionIndex
        ) :: INTEGER AS tx_index,
        ethereum.public.udf_hex_to_int(
            TYPE
        ) :: INTEGER AS TYPE,
        _inserted_timestamp
    FROM
        logs_response
),
new_records AS (
    SELECT
        f.block_number,
        b.block_timestamp,
        f.block_hash,
        f.tx_hash,
        f.origin_from_address,
        f.origin_to_address,
        f.event_index,
        f.contract_address,
        f.topics,
        f.data,
        f.event_removed,
        f.tx_status,
        f.tx_index,
        f.type,
        t.origin_function_signature,
        CASE
            WHEN t.origin_function_signature IS NULL
            OR b.block_timestamp IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        f._log_id,
        f._inserted_timestamp
    FROM
        flat_base f
        LEFT OUTER JOIN {{ ref('silver_goerli__transactions') }}
        t
        ON f.tx_hash = t.tx_hash
        AND f.block_number = t.block_number
        LEFT OUTER JOIN {{ ref('silver_goerli__blocks') }}
        b
        ON f.block_number = b.block_number
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp,
        t.block_hash,
        t.tx_hash,
        t.origin_from_address,
        t.origin_to_address,
        t.event_index,
        t.contract_address,
        t.topics,
        t.data,
        t.event_removed,
        t.tx_status,
        t.tx_index,
        t.type,
        txs.origin_function_signature,
        FALSE AS is_pending,
        t._log_id,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp,
            txs._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver_goerli__transactions') }}
        txs
        ON t.tx_hash = txs.tx_hash
        AND t.block_number = txs.block_number
        INNER JOIN {{ ref('silver_goerli__blocks') }}
        b
        ON t.block_number = b.block_number
    WHERE
        t.is_pending
)
{% endif %}
SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    tx_index,
    TYPE,
    origin_function_signature,
    is_pending,
    _log_id,
    _inserted_timestamp
FROM
    new_records qualify(ROW_NUMBER() over (PARTITION BY block_number, _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    missing_data
{% endif %}
