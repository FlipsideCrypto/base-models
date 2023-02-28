{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    eth_value,
    block_hash,
    gas_price,
    gas_limit,
    input_data,
    -- need receipts for tx status, gas used, L1 gas prices
    tx_type,
    is_system_tx,
    tx_json
FROM
    {{ ref('silver_goerli__transactions') }}