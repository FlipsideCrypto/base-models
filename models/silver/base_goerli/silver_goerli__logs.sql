{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["_log_id"]
) }}

SELECT
    _log_id,
    block_number,
    blockHash AS block_hash,
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
    {{ ref('silver_goerli__receipts_method') }}
WHERE
    tx_hash IS NOT NULL

{% if is_incremental() %}
and
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
