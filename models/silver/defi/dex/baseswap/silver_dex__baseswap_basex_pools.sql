{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['curated']
) }}

WITH created_pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                topics [3] :: STRING
            )
        ) AS fee,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [0] :: STRING
            )
        ) AS tick_spacing,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS pool_address,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp > '2023-08-01'
        AND contract_address = LOWER('0x38015D05f4fEC8AFe15D7cc0386a126574e8077B') -- BASEX V3 FACTORY
        AND topic_0 = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
        AND tx_succeeded
        AND event_removed = 'false'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
initial_info AS (
    SELECT
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING))
        ) AS init_sqrtPriceX96,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [1] :: STRING))
        ) AS init_tick,
        pow(
            1.0001,
            init_tick
        ) AS init_price_1_0_unadj
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            SELECT
                pool_address
            FROM
                created_pools
        )
        AND topic_0 = '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95'
        AND tx_succeeded
        AND event_removed = 'false'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    p.contract_address,
    token0_address,
    token1_address,
    fee,
    (
        fee / 10000
    ) :: FLOAT AS fee_percent,
    tick_spacing,
    pool_address,
    COALESCE(
        init_tick,
        0
    ) AS init_tick,
    p._log_id,
    p.modified_timestamp
FROM
    created_pools p
    LEFT JOIN initial_info i
    ON p.pool_address = i.contract_address qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
