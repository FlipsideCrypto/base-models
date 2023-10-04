{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    id,
    symbol,
    NAME,
    decimals,
    _last_modified_timestamp
FROM
    {{ ref('silver__asset_metadata_priority') }}