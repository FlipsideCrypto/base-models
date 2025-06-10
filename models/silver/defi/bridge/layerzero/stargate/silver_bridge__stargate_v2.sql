{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "contract_address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}
-- busdriven + oftsent
WITH busdriven AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        guid,
        event_index,
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
        {{ ref('silver_bridge__stargate_v2_busdriven') }}
) block_number,
block_timestamp,
tx_hash,
guid,
event_index,
stargate_oft_address,
-- need a reads model to read the token func (0xfc0c546a)
from_address,
to_address,
src_chain_id,
src_chain,
dst_chain_id,
dst_chain,
dst_chain_id_oft,
amount_sent,
payload,
TYPE,
nonce,
sender_contract_address,
receiver_contract_address,
message_type,
message_type_2
