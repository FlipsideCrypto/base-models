{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number AS block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    value_adjusted :: FLOAT AS eth_value,
    VALUE AS eth_value_precise_raw,
    value_adjusted AS eth_value_precise,
    tx_fee,
    gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_fee,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    tx_status AS status,
    r,
    s,
    v
FROM
    {{ ref('silver__transactions') }}
