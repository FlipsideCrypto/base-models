 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX, PERPS'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    symbol,
    digest,
    trader,
    subaccount,
    trade_type,
    order_type,
    market_reduce_flag,
    expiration,
    nonce,
    is_taker,
    price_amount_unadj,
    price_amount,
    amount_unadj,
    amount,
    amount_usd,
    fee_amount_unadj,
    fee_amount,
    base_delta_amount_unadj,
    base_delta_amount,
    quote_delta_amount_unadj,
    quote_delta_amount,
    vertex_perps_id AS ez_perp_trades_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_perps') }}