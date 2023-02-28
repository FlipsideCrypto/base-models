{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    _log_id,
    A.block_number AS block_number,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    contract_address,
    topics,
    data,
    gas_used,
    cumulative_gas_used,
    event_removed,
    tx_status
FROM
    {{ ref('silver_goerli__logs') }} A

