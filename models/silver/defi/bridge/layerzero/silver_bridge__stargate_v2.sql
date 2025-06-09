{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "contract_address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH decoded_logs AS (

    SELECT
        block_number,
        block_timestamp,
        event_index,
        contract_address,
        event_name,
        decoded_log,
        tx_hash
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-01-01'
        AND (
            (
                event_name = 'PacketSent'
                AND contract_address = LOWER('0x1a44076050125825900e736c501f859c50fE728c') -- layerzero endpoint v2
            )
            OR (
                event_name = 'BusDriven'
                AND contract_address = LOWER('0x5634c4a5fed09819e3c46d86a965dd9447d86e47') -- stargate token messaging
            )
        )
),
layerzero AS (
    SELECT
        block_timestamp,
        tx_hash,
        event_index,
        SUBSTR(
            decoded_log :encodedPayload :: STRING,
            3
        ) AS payload,
        SUBSTR(
            payload,
            1,
            2
        ) AS TYPE,
        SUBSTR(
            payload,
            3,
            16
        ) AS nonce,
        utils.udf_hex_to_int(SUBSTR(payload, 19, 8)) AS source_chain_hex,
        '0x' || SUBSTR(SUBSTR(payload, 27, 64), 25) AS sender_ca,
        -- token messaging for stargate in arbitrum
        utils.udf_hex_to_int(SUBSTR(payload, 91, 8)) AS dst_chain_hex,
        '0x' || SUBSTR(SUBSTR(payload, 99, 64), 25) AS receiver_ca,
        SUBSTR(
            payload,
            163,
            64
        ) AS guid,
        SUBSTR(
            payload,
            227,
            2
        ) AS message_type {# SUBSTR(
        payload,
        229,
        4
) AS message_type_2,
'0x' || SUBSTR(SUBSTR(payload, 233, 64), 25) AS dst_chain_receiver #}
FROM
    decoded_logs
WHERE
    event_name = 'PacketSent'
    AND contract_address = LOWER('0x1a44076050125825900e736c501f859c50fE728c') -- layerzero endpoint v2
),
bus_driven_raw AS (
    SELECT
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        decoded_log :dstEid :: INT AS dst_id,
        SUBSTR(
            decoded_log :guid :: STRING,
            3
        ) AS guid,
        decoded_log :numPassengers :: INT AS num_passengers,
        decoded_log :startTicketId :: INT AS start_ticket_id,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS rn,
        start_ticket_id + num_passengers - 1 AS end_ticket_id
    FROM
        decoded_logs
    WHERE
        event_name = 'BusDriven'
        AND contract_address = '0x5634c4a5fed09819e3c46d86a965dd9447d86e47'
),
bus_driven AS (
    SELECT
        r.block_timestamp,
        r.tx_hash,
        r.event_index,
        contract_address,
        r.dst_id,
        start_ticket_id,
        num_passengers,
        end_ticket_id,
        b.ticket_id,
        asset_id,
        dst_receiver_address,
        amount_transferred,
        guid
    FROM
        bus_driven r
        INNER JOIN {{ ref('silver_bridge__stargate_v2_bus') }}
        b
        ON r.dst_id = b.dst_id
        AND b.ticket_id >= start_ticket_id
        AND b.ticket_id <= end_ticket_id
),
final_bus_driven AS (
    SELECT
        block_timestamp,
        tx_hash,
        guid,
        event_index,
        contract_address,
        dst_id,
        start_ticket_id,
        num_passengers,
        end_ticket_id,
        ticket_id,
        asset_id,
        dst_receiver_address,
        amount_transferred,
        payload,
        TYPE,
        nonce,
        source_chain_hex,
        sender_ca,
        dst_chain_hex,
        receiver_ca,
        message_type,
        message_type
    FROM
        bus_driven b
        LEFT JOIN layerzero l
        ON b.tx_hash = l.tx_hash
        AND b.guid = l.guid
),
oft_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        contract_address,
        contract_address AS stargate_adapter_address,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        SUBSTR(
            topic_1,
            3
        ) AS guid,
        '0x' || SUBSTR(
            topic_2,
            27
        ) AS from_address,
        -- src sender
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) AS dst_id,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) AS amount_sent
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-01-01'
        AND topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' --OFTSent
),
final_oft AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        l.event_index,
        contract_address,
        guid,
        from_address,
        -- src sender
        dst_id,
        amount_sent,
        payload,
        TYPE,
        nonce,
        source_chain_hex,
        sender_ca,
        -- token messaging for stargate in arbitrum
        dst_chain_hex,
        receiver_ca,
        message_type,
        SUBSTR(
            payload,
            229,
            4
        ) AS message_type_2,
        '0x' || SUBSTR(SUBSTR(payload, 233, 64), 25) AS dst_chain_receiver
    FROM
        oft_raw o
        INNER JOIN layerzero l
        ON o.tx_hash = l.tx_hash
        AND o.guid = l.guid
)
SELECT
    block_timestamp,
    tx_hash,
    guid,
    event_index,
    contract_address,
    -- either stargate adapter(taxi) or stargate token messaging(bus)
    dst_id,
    --start_ticket_id,
    --num_passengers,
    --end_ticket_id,
    --ticket_id,
    asset_id,
    dst_receiver_address,
    amount_transferred,
    payload,
    TYPE,
    nonce,
    source_chain_hex,
    sender_ca,
    dst_chain_hex,
    receiver_ca,
    message_type,
    message_type
FROM
    final_bus_driven
UNION ALL
SELECT
    tx_hash,
    l.event_index,
    contract_address,
    guid,
    from_address,
    -- src sender
    dst_id,
    amount_sent,
    payload,
    TYPE,
    nonce,
    source_chain_hex,
    sender_ca,
    -- token messaging for stargate in arbitrum
    dst_chain_hex,
    receiver_ca,
    message_type,
    SUBSTR(
        payload,
        229,
        4
    ) AS message_type_2,
    '0x' || SUBSTR(SUBSTR(payload, 233, 64), 25) AS dst_chain_receiver
FROM
    final_oft
