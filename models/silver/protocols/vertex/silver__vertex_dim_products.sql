{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'product_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH logs_pull AS (

    SELECT
        topics,
        contract_address,
        DATA,
        tx_hash,
        block_number,
        block_timestamp,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x3286b0394bf1350245290b7226c92ed186bd716f28938e62dbb895298f018172'

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
new_prod AS (
    SELECT
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: STRING AS product_id,
        tx_hash,
        block_number,
        block_timestamp,
        _inserted_timestamp,
        _log_id
    FROM
        logs_pull
    WHERE
        topics [0] :: STRING = '0x3286b0394bf1350245290b7226c92ed186bd716f28938e62dbb895298f018172'
),
api_pull AS (
    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://gateway.base-prod.vertexprotocol.com/api/v2/assets'
            )
        ) :data AS response
),
api_lateral_flatten AS (
    SELECT
        r.value
    FROM
        api_pull,
        LATERAL FLATTEN (response) AS r
),
product_metadata AS (
    SELECT
        VALUE :product_id AS product_id,
        VALUE :ticker_id AS ticker_id,
        VALUE :symbol AS symbol,
        VALUE :name AS NAME,
        VALUE :market_type AS market_type,
        VALUE :taker_fee AS taker_fee,
        VALUE :maker_fee AS maker_fee
    FROM
        api_lateral_flatten
),
FINAL AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.product_id,
        CASE
            WHEN l.product_id % 2 = 0 THEN 'perp'
            ELSE 'spot'
        END AS product_type,
        CASE
            WHEN l.product_id = 0 THEN 'USDB'
            ELSE p.ticker_id :: STRING
        END AS ticker_id,
        p.symbol :: STRING AS symbol,
        p.name :: STRING AS NAME,
        CASE
            WHEN l.product_id = 0 THEN NULL
            ELSE FLOOR((l.product_id - 1) / 2)
        END AS health_group,
        CASE
            WHEN SPLIT(
                p.symbol,
                '-'
            ) [0] = 'WBTC' THEN 'BTC'
            WHEN SPLIT(
                p.symbol,
                '-'
            ) [0] = 'WETH' THEN 'ETH'
            ELSE SPLIT(
                p.symbol,
                '-'
            ) [0]
        END AS health_group_symbol,
        p.taker_fee,
        p.maker_fee,
        _inserted_timestamp,
        _log_id
    FROM
        new_prod l
        LEFT JOIN product_metadata p
        ON l.product_id = p.product_id
    WHERE
        p.ticker_id IS NOT NULL
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','product_id']
    ) }} AS vertex_products_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY product_id
ORDER BY
    _inserted_timestamp DESC)) = 1
