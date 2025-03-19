{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH swaps_base AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS recipient,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: FLOAT AS amount0_unadj,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: FLOAT AS amount1_unadj,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) :: FLOAT AS sqrtPriceX96,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [3] :: STRING
        ) :: FLOAT AS liquidity,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) :: FLOAT AS tick,
        token0_address,
        token1_address,
        pool_address,
        tick_spacing,
        fee,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__sushi_pools_v3') }}
        p
        ON p.pool_address = l.contract_address
    WHERE
        l.block_timestamp :: DATE >= '2023-04-01'
        AND topic_0 = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND tx_succeeded
        AND event_removed = 'false'

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_address,
    recipient,
    recipient AS tx_to,
    sender,
    fee,
    tick,
    tick_spacing,
    liquidity,
    token0_address,
    token1_address,
    amount0_unadj,
    amount1_unadj,
    CASE
        WHEN amount0_unadj > 0 THEN ABS(amount0_unadj)
        ELSE ABS(amount1_unadj)
    END AS amount_in_unadj,
    CASE
        WHEN amount0_unadj < 0 THEN ABS(amount0_unadj)
        ELSE ABS(amount1_unadj)
    END AS amount_out_unadj,
    CASE
        WHEN amount0_unadj > 0 THEN token0_address
        ELSE token1_address
    END AS token_in,
    CASE
        WHEN amount0_unadj < 0 THEN token0_address
        ELSE token1_address
    END AS token_out,
    _log_id,
    modified_timestamp
FROM
    swaps_base qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
