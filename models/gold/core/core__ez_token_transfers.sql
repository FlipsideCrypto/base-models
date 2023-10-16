{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','non_realtime','reorg']
) }}

WITH base_transfers AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        raw_amount,
        _log_id,
        _inserted_timestamp,
        raw_amount_precise
    FROM
        {{ ref('silver__transfers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    t.contract_address,
    from_address,
    to_address,
    raw_amount_precise,
    raw_amount,
    IFF(
        C.token_decimals IS NOT NULL,
        utils.udf_decimal_adjust(
            raw_amount_precise,
            C.token_decimals
        ),
        NULL
    ) AS amount_precise,
    amount_precise :: FLOAT AS amount,
    IFF(
        C.token_decimals IS NOT NULL
        AND price IS NOT NULL,
        amount * price,
        NULL
    ) AS amount_usd,
    C.token_decimals AS decimals,
    C.token_symbol AS symbol,
    price AS token_price,
    C.token_decimals IS NOT NULL AS has_decimal,
    C.token_symbol IS NOT NULL AS has_symbol,
    price IS NOT NULL AS has_price,
    _log_id,
    _inserted_timestamp
FROM
    base_transfers t
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON t.contract_address = p.token_address
    AND DATE_TRUNC(
        'hour',
        t.block_timestamp
    ) = HOUR
    LEFT JOIN {{ ref('silver__contracts') }} C USING (contract_address)
