{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    price,
    is_imputed,
    provider,
    _last_modified_timestamp
FROM
    {{ ref('silver__hourly_prices_all_providers') }}
