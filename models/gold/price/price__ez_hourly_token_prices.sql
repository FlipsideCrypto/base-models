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
	COALESCE (
        hourly_prices_priority_id,
        {{ dbt_utils.generate_surrogate_key(
            ['token_address', 'hour']
        ) }}
    ) AS ez_hourly_token_prices_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__hourly_prices_priority') }}
