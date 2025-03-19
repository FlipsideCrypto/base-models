{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['curated']
) }}

WITH pools AS (

    SELECT
        pool_address,
        token0_address,
        token1_address,
        fee,
        tick_spacing
    FROM
        {{ ref('silver_dex__baseswap_basex_pools') }}
),
base_swaps AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        event_index,
        'Swap' AS event_name,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS recipient,
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
        CASE
            WHEN tx_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        block_timestamp :: DATE >= '2023-08-01'
        AND topic_0 = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND tx_succeeded
        AND event_removed = 'false'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    'baseswap-basex' AS platform,
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    contract_address as pool_address,
    recipient,
    sender,
    fee,
    tick,
    tick_spacing,
    liquidity,
    event_name,
    event_index,
    token0_address,
    token1_address,
    _log_id,
    modified_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    amount0_unadj,
    amount1_unadj
FROM
    base_swaps
    INNER JOIN pools
    ON pools.pool_address = base_swaps.contract_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
