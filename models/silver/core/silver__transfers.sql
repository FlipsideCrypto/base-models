{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH logs AS (

    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address :: STRING AS contract_address,
        CONCAT('0x', SUBSTR(topics [1], 27, 40)) :: STRING AS from_address,
        CONCAT('0x', SUBSTR(topics [2], 27, 40)) :: STRING AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
        raw_amount_precise :: FLOAT AS raw_amount,
        event_index,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
new_records AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
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
        logs t
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON t.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = HOUR
        LEFT JOIN {{ ref('silver__contracts') }} C USING (contract_address)
    WHERE
        raw_amount IS NOT NULL
        AND to_address IS NOT NULL
        AND from_address IS NOT NULL
)

{% if is_incremental() %},
heal_missing_contracts AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.tx_hash,
        t.event_index,
        t.origin_function_signature,
        t.origin_from_address,
        t.origin_to_address,
        t.contract_address,
        t.from_address,
        t.to_address,
        t.raw_amount_precise,
        t.raw_amount,
        IFF(
            C.token_decimals IS NOT NULL,
            utils.udf_decimal_adjust(
                t.raw_amount_precise,
                C.token_decimals
            ),
            NULL
        ) AS amount_precise_heal,
        amount_precise_heal :: FLOAT AS amount_heal,
        IFF(
            C.token_decimals IS NOT NULL
            AND p.price IS NOT NULL,
            amount_heal * p.price,
            NULL
        ) AS amount_usd_heal,
        C.token_decimals AS decimals,
        C.token_symbol AS symbol,
        p.price AS token_price,
        C.token_decimals IS NOT NULL AS has_decimal,
        C.token_symbol IS NOT NULL AS has_symbol,
        p.price IS NOT NULL AS has_price,
        t._log_id,
        t._inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__contracts') }} C USING (contract_address)
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON t.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = HOUR
    WHERE
        t.decimals IS NULL
        AND C.token_decimals IS NOT NULL
        AND t._inserted_timestamp < (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '12 hours'
            FROM
                {{ this }}
        )
),
heal_missing_prices AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.tx_hash,
        t.event_index,
        t.origin_function_signature,
        t.origin_from_address,
        t.origin_to_address,
        t.contract_address,
        t.from_address,
        t.to_address,
        t.raw_amount_precise,
        t.raw_amount,
        t.amount_precise,
        t.amount,
        IFF(
            t.decimals IS NOT NULL
            AND p.price IS NOT NULL,
            t.amount * p.price,
            NULL
        ) AS amount_usd_heal,
        t.decimals,
        t.symbol,
        p.price AS token_price,
        t.decimals IS NOT NULL AS has_decimal,
        t.symbol IS NOT NULL AS has_symbol,
        p.price IS NOT NULL AS has_price,
        t._log_id,
        t._inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON t.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = HOUR
    WHERE
        t.decimals IS NOT NULL
        AND t.token_price IS NULL
        AND p.price IS NOT NULL
        AND t._inserted_timestamp < (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '12 hours'
            FROM
                {{ this }}
        )
)
{% endif %}
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
    _log_id,
    _inserted_timestamp
FROM
    new_records

{% if is_incremental() %}
UNION ALL
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
    amount_precise_heal AS amount_precise,
    amount_heal AS amount,
    amount_usd_heal AS amount_usd,
    decimals,
    symbol,
    token_price,
    has_decimal,
    has_symbol,
    has_price,
    _log_id,
    _inserted_timestamp
FROM
    heal_missing_contracts
UNION ALL
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
    amount_usd_heal AS amount_usd,
    decimals,
    symbol,
    token_price,
    has_decimal,
    has_symbol,
    has_price,
    _log_id,
    _inserted_timestamp
FROM
    heal_missing_prices
{% endif %}
