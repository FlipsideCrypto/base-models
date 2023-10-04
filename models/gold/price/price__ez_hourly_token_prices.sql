{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed,
    _last_modified_timestamp
FROM
    {{ ref('silver__hourly_prices_priority') }}
