{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    t.block_number,
    t.block_hash,
    block_timestamp,
    t.tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    eth_value,
    gas_price,
    gas_limit,
    input_data,
    tx_status,
    gas_used,
    -- L1 gas prices
    tx_type,
    is_system_tx,
    tx_json
FROM
    {{ ref('silver_goerli__transactions') }} t
LEFT JOIN {{ ref('silver_goerli__logs') }} l
    ON t.tx_hash = l.tx_hash