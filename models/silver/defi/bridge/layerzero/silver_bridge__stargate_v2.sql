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
        tx_hash,
        origin_from_address
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2025-03-01'
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
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
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
    AND sender_ca = '0x5634c4a5fed09819e3c46d86a965dd9447d86e47' -- stargate token messaging. Ensures adapter sends
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
        start_ticket_id + num_passengers - 1 AS end_ticket_id,
        origin_from_address
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
        r.guid,
        origin_from_address
    FROM
        bus_driven_raw r
        INNER JOIN {{ ref('silver_bridge__stargate_v2_busrode') }}
        -- join with busRode
        b
        ON r.dst_id = b.dst_id
        AND b.ticket_id >= start_ticket_id
        AND b.ticket_id <= end_ticket_id
),
final_bus_driven AS (
    SELECT
        b.block_timestamp,
        b.tx_hash,
        guid,
        b.event_index,
        b.contract_address,
        dst_id,
        start_ticket_id,
        num_passengers,
        end_ticket_id,
        ticket_id,
        asset_id,
        asset AS asset_symbol,
        -- from the list
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
        origin_from_address
    FROM
        bus_driven b
        INNER JOIN layerzero l USING (
            tx_hash,
            guid
        )
        LEFT JOIN {{ ref('silver_bridge__stargate_asset_id') }}
        s
        ON asset_id = s.id
        AND s.chain = 'Base'
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
        block_timestamp :: DATE >= '2025-03-01'
        AND topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' --OFTSent
),
final_oft AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        tx_hash,
        l.event_index,
        l.contract_address,
        stargate_adapter_address,
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
        INNER JOIN layerzero l USING (
            tx_hash,
            guid
        )
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
    origin_from_address AS from_address,
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
    'bus' AS bridge_type
FROM
    final_bus_driven
UNION ALL
SELECT
    block_timestamp,
    tx_hash,
    guid,
    event_index,
    stargate_adapter_address,
    -- adapter address, the CA of the adapter
    dst_id,
    stargate_adapter_address AS asset_id,
    -- this supposed to be asset id
    from_address,
    -- src sender
    dst_chain_receiver AS dst_receiver_address,
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
    'oft' AS bridge_type
FROM
    final_oft
