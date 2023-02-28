{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["_log_id"]
) }}

SELECT
	block_number,
    blockHash AS block_hash,
    ethereum.public.udf_hex_to_int(
    	cumulativeGasUsed) :: INTEGER AS cumulative_gas_used,
    ethereum.public.udf_hex_to_int(
    	effectiveGasPrice) :: INTEGER AS effective_gas_price,
    origin_from_address,
    ethereum.public.udf_hex_to_int(
    	gasUsed) :: INTEGER AS gas_used,
    contract_address,
    data,
    event_index,
    CASE
        WHEN removed = 'true' THEN TRUE
        ELSE FALSE
    END AS event_removed,
    topics,
    tx_hash,
    ethereum.public.udf_hex_to_int(
    	transactionIndex) :: INTEGER AS tx_index,
    CASE
        WHEN status = '0x1' THEN 'SUCCESS'
        ELSE 'FAIL'
    END AS tx_status,
    CASE
        WHEN LEN(origin_to_address) <= 0 THEN NULL
        ELSE origin_to_address
    END AS origin_to_address,
    ethereum.public.udf_hex_to_int(
    	type) :: INTEGER AS type,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_goerli__logs_method') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
