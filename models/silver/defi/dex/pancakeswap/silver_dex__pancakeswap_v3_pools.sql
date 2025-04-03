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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1_address,
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
        ) AS tickSpacing,
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
        contract_address = '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865' --factory
        AND topic_0 = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' --paircreated
        AND tx_succeeded
        AND event_removed = FALSE

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
    contract_address,
    token0_address,
    token1_address,
    fee :: INTEGER AS fee,
    (
        fee / 10000
    ) :: FLOAT AS fee_percent,
    tickSpacing AS tick_spacing,
    pool_address,
    _log_id,
    modified_timestamp
FROM
    created_pools qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
