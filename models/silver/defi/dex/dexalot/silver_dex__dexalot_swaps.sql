{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_swaps AS (

    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topics [1] :: STRING AS nouceAndMeta,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS taker,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS destTrader,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [2] :: STRING
            )
        ) AS destChainId,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS destAsset,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS srcAsset,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [5] :: STRING
            )
        ) AS srcAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [6] :: STRING
            )
        ) AS destAmount,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x1fd108cf42a59c635bd4703b8dbc8a741ff834be'
        AND topic_0 = '0x68eb6d948c037c94e470f9a5b288dd93debbcd9342635408e66cb0211686f7f7'
        AND destChainId = 8453
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
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    'SwapExecuted' AS event_name,
    srcAmount AS amount_in_unadj,
    destAmount AS amount_out_unadj,
    srcAsset AS token_in,
    destAsset AS token_out,
    origin_from_address AS sender,
    taker AS recipient,
    destTrader AS tx_to,
    event_index,
    'dexalot' AS platform,
    _log_id,
    modified_timestamp
FROM
    base_swaps
