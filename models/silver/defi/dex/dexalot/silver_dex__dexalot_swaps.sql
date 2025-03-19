{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_swaps AS (

    SELECT
        'dexalot-simpleswap' AS platform,
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        decoded_log,
        TRY_TO_NUMBER(
            decoded_log :destAmount :: STRING
        ) AS destAmount,
        decoded_log :destAsset :: STRING AS destAsset,
        TRY_TO_NUMBER(
            decoded_log :destChainId :: STRING
        ) AS destChainId,
        decoded_log :destTrader :: STRING AS destTrader,
        decoded_log :nonceAndMeta :: STRING AS nouceAndMeta,
        TRY_TO_NUMBER(
            decoded_log :srcAmount :: STRING
        ) AS srcAmount,
        decoded_log :srcAsset :: STRING AS srcAsset,
        decoded_log :taker :: STRING AS taker,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
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
