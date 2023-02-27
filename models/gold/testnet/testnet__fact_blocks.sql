{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
    block_timestamp,
    network,
    blockchain,
    tx_count,
    difficulty,
    total_difficulty,
    extra_data,
    gas_limit,
    gas_used,
    HASH,
    parent_hash,
    receipts_root,
    sha3_uncles,
    SIZE,
    uncle_blocks,
    block_header_json
FROM
    {{ ref('silver__blocks') }} A

