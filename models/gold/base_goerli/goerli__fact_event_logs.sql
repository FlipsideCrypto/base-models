{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    _log_id,
    l.block_number,
    l.block_hash,
    b.block_timestamp,
    l.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    topics,
    data,
    event_removed,
    l.tx_status,
    tx_index,
    l.gas_used,
    cumulative_gas_used,
    effective_gas_price,
    type
FROM
    {{ ref('silver_goerli__logs') }} l
LEFT JOIN {{ ref('silver_goerli__blocks') }} b
        ON l.block_number = b.block_number
LEFT JOIN {{ ref('silver_goerli__transactions') }} t
        ON l.tx_hash = t.tx_hash

