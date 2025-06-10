{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "contract_address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH bus_driven_raw AS (

    SELECT
        block_number,
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
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = '0x5634c4a5fed09819e3c46d86a965dd9447d86e47'
        AND event_name = 'BusDriven'
        AND block_timestamp :: DATE >= '2024-01-01'
),
bus_driven AS (
    SELECT
        r.block_number,
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
        asset_name,
        from_address,
        dst_receiver_address,
        amount_transferred,
        r.guid
    FROM
        bus_driven_raw r
        INNER JOIN {{ ref('silver_bridge__stargate_v2_busrode') }}
        -- join with busRode
        b
        ON r.dst_id = b.dst_id
        AND b.ticket_id >= start_ticket_id
        AND b.ticket_id <= end_ticket_id
),
layerzero AS (
    SELECT
        tx_hash,
        guid,
        payload,
        TYPE,
        nonce,
        src_chain_id,
        src_chain,
        sender_contract_address,
        dst_chain_id,
        dst_chain,
        receiver_contract_address,
        message_type
    FROM
        {{ ref('silver_bridge__layerzero_v2_packet') }}
)
SELECT
    b.block_number,
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
    asset_name,
    from_address,
    dst_receiver_address,
    amount_transferred,
    payload,
    TYPE,
    nonce,
    src_chain_id,
    src_chain,
    sender_contract_address,
    dst_chain_id,
    dst_chain,
    receiver_contract_address,
    message_type
FROM
    bus_driven b
    INNER JOIN layerzero l USING (
        tx_hash,
        guid
    )
