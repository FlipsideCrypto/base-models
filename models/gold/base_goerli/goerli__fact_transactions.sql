{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_hash,
    block_timestamp,
    tx_hash,
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
    l1_fee_scalar,
    l1_gas_used,
    l1_gas_price,
    tx_fee,
    tx_type,
    is_system_tx,
    tx_json
FROM
    {{ ref('silver_goerli__transactions') }} 