{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'across' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_flat :"depositId" :: STRING
        ) AS depositId,
        decoded_flat :"depositor" :: STRING AS depositor,
        TRY_TO_NUMBER(
            decoded_flat :"destinationChainId" :: STRING
        ) AS destinationChainId,
        decoded_flat :"message" :: STRING AS message,
        TRY_TO_NUMBER(
            decoded_flat :"originChainId" :: STRING
        ) AS originChainId,
        decoded_flat :"originToken" :: STRING AS originToken,
        TRY_TO_TIMESTAMP(
            decoded_flat :"quoteTimestamp" :: STRING
        ) AS quoteTimestamp,
        decoded_flat :"recipient" :: STRING AS recipient,
        TRY_TO_NUMBER(
            decoded_flat :"relayerFeePct" :: STRING
        ) AS relayerFeePct,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0xafc4df6845a4ab948b492800d3d8a25d538a102a2bc07cd01f1cfa097fddcff6'
        AND contract_address = '0x09aea4b2242abc8bb4bb78d537a67a245a7bec64'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    event_removed,
    tx_status,
    contract_address AS bridge_address,
    name AS platform,
    depositor AS sender,
    recipient AS receiver,
    recipient AS destination_chain_receiver,
    destinationChainId AS destination_chain_id,
    amount,
    depositId AS deposit_id,
    message,
    originChainId AS origin_chain_id,
    originToken AS token_address,
    quoteTimestamp AS quote_timestamp,
    relayerFeePct AS relayer_fee_pct,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
