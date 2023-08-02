{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    VALUE AS eth_value,
    eth_value_precise_raw,
    eth_value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_fee,
    OBJECT_CONSTRUCT(
        'l1_state_batch_index',
        state_batch_index,
        'l1_state_batch_root',
        state_batch_root,
        'l1_state_root_tx_hash',
        state_tx_hash
    ) AS l1_submission_details,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    tx_status AS status,
    r,
    s,
    v
FROM
    <<<<<<< head {{ ref('silver__transactions') }} ======= {{ ref('silver__transactions') }} A
    LEFT JOIN {{ ref('silver__state_hashes') }}
    b
    ON A.block_number = b.block_number >>>>>>> e5214d86a4dc32d88cb548a7f4d0545b79b536e6
