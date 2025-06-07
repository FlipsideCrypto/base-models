{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "contract_address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

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
    {{ ref('core__ez_decoded_event_logs') }}
WHERE
    1 = 1 --tx_hash = '0xd726c07902df1c68f23b911a2c5c3d2e241ebc5da155b22253f3b3ebc2e3eb1c'
    AND block_timestamp :: DATE >= '2024-01-01'
    AND event_name = 'PacketSent'
    AND contract_address = LOWER('0x1a44076050125825900e736c501f859c50fE728c') -- layerzero endpoint v2
    /*
            
            bus rode 
            https://basescan.org/tx/0x10bebb3d970a50ca3dcb91c57559915c8c79490b17f023823ec92dbb463572c3#eventlog 
            
            bus driven
            https://basescan.org/tx/0x1b19373f9153a3271880a404b62a2eb961e06d3b18ac9fe6e4dd86032bc63d12#eventlog
            
            passenger follows this format:
            000D – asset id 
            000000000000000000000000CADC222B22BDB30147BB8FB9726AA9E00A01441C – receiver 
            000000000001EFE9 – amountSD 
            00 –nativeDrop 
            
            
             uint16 assetId;
                bytes32 receiver;
                uint64 amountSD;
                bool nativeDrop;
             
            
             assetIDs from stargate
            
            */
