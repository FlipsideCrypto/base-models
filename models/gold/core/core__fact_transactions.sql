{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    CASE
        WHEN tx_status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    -- new column
    tx_type,
    -- new column
    nonce,
    POSITION AS tx_position,
    -- new column
    input_data,
    gas_price,
    gas_used,
    gas AS gas_limit,
    cumulative_gas_used,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_fee_precise,
    l1_fee,
    OBJECT_CONSTRUCT(
        'l1_state_batch_index',
        state_batch_index,
        'l1_state_batch_root',
        state_batch_root,
        'l1_state_root_tx_hash',
        state_tx_hash
    ) AS l1_submission_details,
    -- remove this later
    r,
    s,
    v,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_transactions_id,
    COALESCE(
        A.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        A.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    tx_status AS status,
    --deprecate
    POSITION,
    -- deprecate
    deposit_nonce,
    -- deprecate
    deposit_receipt_version -- deprecate
FROM
    {{ ref('silver__transactions') }} A
    LEFT JOIN {{ ref('silver__state_hashes') }}
    -- remove this join later
    b
    ON A.block_number = b.block_number
