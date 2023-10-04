{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    token_name AS contract_name,
    event_name,
    decoded_flat AS decoded_log,
    decoded_data AS full_decoded_log,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    GREATEST(
        d._last_modified_timestamp,
        C._last_modified_timestamp
    ) AS _last_modified_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
    d
    LEFT JOIN {{ ref('silver__contracts') }} C USING (contract_address)
