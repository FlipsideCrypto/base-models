{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "contract_address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH base_contracts AS (

    SELECT
        contract_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-05-01'
        AND topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' -- OFTsent
        qualify ROW_NUMBER() over (
            PARTITION BY contract_address
            ORDER BY
                block_timestamp DESC
        ) = 1
)
SELECT
    b.contract_address,
    p.token_address,
    p.decimals,
    p.shared_decimals,
    p.endpoint,
    p.owner,
    p.token_name,
    p.token_symbol
FROM
    base_contracts b
    INNER JOIN {{ ref('silver_bridge__stargate_pools') }}
    p
    ON b.contract_address = p.pool_address
WHERE
    p.token_address IS NOT NULL
    AND p.decimals IS NOT NULL
