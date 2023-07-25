{{ config (
    materialized = "table",
    unique_key = "batch_id"
) }}

WITH ranges AS (

    SELECT
        batch_id AS group_id,
        fromBlock,
        toBlock,
        contract_address
    FROM
        {{ ref('silver__eth_goerli_block_ranges') }}
        JOIN {{ ref('silver__eth_goerli_contracts') }}
        ON 1 = 1
)
SELECT
    ROW_NUMBER() over (
        ORDER BY
            group_id,
            contract_address
    ) AS batch_id,*
FROM
    ranges
