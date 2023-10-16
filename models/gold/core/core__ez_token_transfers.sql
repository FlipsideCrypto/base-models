{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    raw_amount_precise,
    raw_amount,
    amount_precise,
    amount,
    amount_usd,
    decimals,
    symbol,
    token_price,
    has_decimal,
    has_symbol,
    has_price,
    _log_id
FROM
    {{ ref('silver__transfers') }}
