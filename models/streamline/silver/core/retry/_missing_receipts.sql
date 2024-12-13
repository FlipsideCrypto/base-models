{{ config (
    materialized = "ephemeral"
) }}

WITH lookback AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
)
SELECT
    DISTINCT t.block_number AS block_number
FROM
    {{ ref("silver__transactions") }}
    t
    LEFT JOIN {{ ref("silver__receipts") }}
    r USING (
        block_number,
        block_hash,
        tx_hash
    )
WHERE
    r.tx_hash IS NULL
    AND t.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND t.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND (
        r._inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
        OR r._inserted_timestamp IS NULL)
        AND t.tx_hash <> '0x13b126388e78adc0fff1b40888b2cd87e6ec0d6c3c9838ee26119b81173bcf25'
