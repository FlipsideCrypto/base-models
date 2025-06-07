{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "contract_address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH logs AS (

    SELECT
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-01-01'
        AND (
            (
                contract_address = LOWER('0x5634c4a5FEd09819E3c46D86A965Dd9447d86e47') -- stargate token messaging
                AND topic_0 = '0x15955c5a4cc61b8fbb05301bce47fd31c0e6f935e1ab97fdac9b134c887bb074' --busRode
            )
            OR topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' --OFTSent
        )
),
oft_sent AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index AS oft_sent_index,
        LEAD(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS next_oft_sent_index,
        contract_address AS stargate_adapter_address,
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
        logs
    WHERE
        topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a'
),
bus_raw AS (
    SELECT
        tx_hash,
        event_index AS bus_rode_index,
        LEAD(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS next_bus_rode_index,
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) AS bus_dst_id,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) AS ticket_id,
        regexp_substr_all(SUBSTR(DATA, 195), '.{64}') AS passenger_raw,
        utils.udf_hex_to_int(
            passenger_raw [1] :: STRING
        ) * 2 AS passenger_length,
        SUBSTR(
            DATA,
            323,
            passenger_length
        ) AS passenger_final,
        SUBSTR(
            passenger_final,
            1,
            4
        ) AS asset_id,
        '0x' || SUBSTR(SUBSTR(passenger_final, 5, 64), 25) AS dst_receiver_address,
        utils.udf_hex_to_int(SUBSTR(passenger_final, 69, 16)) AS amount_transferred,
        SUBSTR(
            passenger_final,
            85,
            2
        ) AS is_native_drop
    FROM
        logs
    WHERE
        topic_0 = '0x15955c5a4cc61b8fbb05301bce47fd31c0e6f935e1ab97fdac9b134c887bb074'
)
SELECT
    o.block_timestamp,
    o.tx_hash,
    o.oft_sent_index,
    b.bus_rode_index,
    stargate_adapter_address,
    guid,
    from_address,
    -- might need to use the origin from address as sender instead. because this address is the address taht sent to the token messaging contract
    dst_id,
    amount_sent,
    bus_dst_id,
    ticket_id,
    asset_id,
    dst_receiver_address,
    amount_transferred,
    is_native_drop
FROM
    oft_sent o
    INNER JOIN bus_raw b
    ON o.tx_hash = b.tx_hash
    AND o.oft_sent_index > b.bus_rode_index
    AND (
        o.oft_sent_index < b.next_bus_rode_index
        OR b.next_bus_rode_index IS NULL
    )
