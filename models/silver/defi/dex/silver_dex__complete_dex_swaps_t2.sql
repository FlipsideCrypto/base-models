-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['curated','reorg']
) }}

WITH sushi AS (

  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    'Swap' AS event_name,
    amount0_unadj,
    amount1_unadj,
    token0_address,
    token1_address,
    sender,
    recipient AS tx_to,
    event_index,
    'sushiswap' AS platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushi_swaps') }}

{% if is_incremental() and 'sushi' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dackie AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    'Swap' AS event_name,
    amount0_unadj,
    amount1_unadj,
    token0_address,
    token1_address,
    sender,
    recipient AS tx_to,
    event_index,
    'dackieswap' AS platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__dackie_swaps') }}

{% if is_incremental() and 'dackie' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
    FROM
      {{ this }}
  )
{% endif %}
),
univ3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    'Swap' AS event_name,
    amount0_unadj,
    amount1_unadj,
    token0_address,
    token1_address,
    sender,
    recipient AS tx_to,
    event_index,
    'uniswap-v3' AS platform,
    'v3' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__univ3_swaps') }}

{% if is_incremental() and 'univ3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
    FROM
      {{ this }}
  )
{% endif %}
),
--union all custom, type 2 dex CTEs here
all_dex AS (
  SELECT
    *
  FROM
    univ3
  UNION ALL
  SELECT
    *
  FROM
    sushi
  UNION ALL
  SELECT
    *
  FROM
    dackie
),
complete_dex_swaps_t2 AS (
  SELECT
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    s.contract_address,
    event_name,
    token0_address,
    token1_address,
    amount0_unadj,
    amount1_unadj,
    amount0_unadj / pow(10, COALESCE(c1.token_decimals, 18)) AS amount0_adjusted,
    amount1_unadj / pow(10, COALESCE(c2.token_decimals, 18)) AS amount1_adjusted,
    CASE
      WHEN c1.token_decimals IS NOT NULL THEN p1.price * amount0_adjusted
    END AS amount0_usd,
    CASE
      WHEN c2.token_decimals IS NOT NULL THEN p2.price * amount1_adjusted
    END AS amount1_usd,
    CASE
      WHEN amount0_unadj > 0 THEN token0_address
      ELSE token1_address
    END AS token_in,
    CASE
      WHEN amount0_unadj > 0 THEN c1.token_decimals
      ELSE c2.token_decimals
    END AS decimals_in,
    CASE
      WHEN amount0_unadj > 0 THEN c1.token_symbol
      ELSE c2.token_symbol
    END AS symbol_in,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_in_unadj,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_in,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_in_usd,
    CASE
      WHEN amount0_unadj < 0 THEN token0_address
      ELSE token1_address
    END AS token_out,
    CASE
      WHEN amount0_unadj < 0 THEN c1.token_decimals
      ELSE c2.token_decimals
    END AS decimals_out,
    CASE
      WHEN amount0_unadj < 0 THEN c1.token_symbol
      ELSE c2.token_symbol
    END AS symbol_out,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_out_unadj,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_out,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_out_usd,
    lp.pool_name AS pool_name,
    sender,
    tx_to,
    s.event_index,
    s.platform,
    s.version,
    s._log_id,
    s._inserted_timestamp
  FROM
    all_dex s
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON c1.contract_address = s.token0_address
    LEFT JOIN {{ ref('silver__contracts') }}
    c2
    ON c2.contract_address = s.token1_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON s.token0_address = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p2
    ON s.token1_address = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON s.contract_address = lp.pool_address
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    t0.block_number,
    t0.block_timestamp,
    t0.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    t0.contract_address,
    event_name,
    token0_address,
    token1_address,
    amount0_unadj,
    amount1_unadj,
    amount0_unadj / pow(10, COALESCE(c1.token_decimals, 18)) AS amount0_adjusted,
    amount1_unadj / pow(10, COALESCE(c2.token_decimals, 18)) AS amount1_adjusted,
    CASE
      WHEN c1.token_decimals IS NOT NULL THEN p1.price * amount0_adjusted
    END AS amount0_usd,
    CASE
      WHEN c2.token_decimals IS NOT NULL THEN p2.price * amount1_adjusted
    END AS amount1_usd,
    CASE
      WHEN amount0_unadj > 0 THEN token0_address
      ELSE token1_address
    END AS token_in,
    CASE
      WHEN amount0_unadj > 0 THEN c1.token_decimals
      ELSE c2.token_decimals
    END AS decimals_in,
    CASE
      WHEN amount0_unadj > 0 THEN c1.token_symbol
      ELSE c2.token_symbol
    END AS symbol_in,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_in_unadj,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_in,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_in_usd,
    CASE
      WHEN amount0_unadj < 0 THEN token0_address
      ELSE token1_address
    END AS token_out,
    CASE
      WHEN amount0_unadj < 0 THEN c1.token_decimals
      ELSE c2.token_decimals
    END AS decimals_out,
    CASE
      WHEN amount0_unadj < 0 THEN c1.token_symbol
      ELSE c2.token_symbol
    END AS symbol_out,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_out_unadj,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_out,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_out_usd,
    lp.pool_name AS pool_name,
    sender,
    tx_to,
    t0.event_index,
    t0.platform,
    t0.version,
    t0._log_id,
    t0._inserted_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON c1.contract_address = t0.token0_address
    LEFT JOIN {{ ref('silver__contracts') }}
    c2
    ON c2.contract_address = t0.token1_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON t0.token0_address = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p2
    ON t0.token1_address = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON t0.contract_address = lp.pool_address
  WHERE
    t0.block_number IN (
      SELECT
        DISTINCT t1.block_number AS block_number
      FROM
        {{ this }}
        t1
      WHERE
        t1.decimals_in IS NULL
        AND t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('silver__contracts') }} C
          WHERE
            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND C.token_decimals IS NOT NULL
            AND C.contract_address = t1.token_in)
        )
        OR t0.block_number IN (
          SELECT
            DISTINCT t2.block_number AS block_number
          FROM
            {{ this }}
            t2
          WHERE
            t2.decimals_out IS NULL
            AND t2._inserted_timestamp < (
              SELECT
                MAX(
                  _inserted_timestamp
                ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
              FROM
                {{ this }}
            )
            AND EXISTS (
              SELECT
                1
              FROM
                {{ ref('silver__contracts') }} C
              WHERE
                C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                AND C.token_decimals IS NOT NULL
                AND C.contract_address = t2.token_out)
            )
            OR t0.block_number IN (
              SELECT
                DISTINCT t3.block_number AS block_number
              FROM
                {{ this }}
                t3
              WHERE
                t3.amount_in_usd IS NULL
                AND t3._inserted_timestamp < (
                  SELECT
                    MAX(
                      _inserted_timestamp
                    ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
                  FROM
                    {{ this }}
                )
                AND EXISTS (
                  SELECT
                    1
                  FROM
                    {{ ref('silver__complete_token_prices') }}
                    p
                  WHERE
                    p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND p.price IS NOT NULL
                    AND p.token_address = t3.token_in
                    AND p.hour = DATE_TRUNC(
                      'hour',
                      t3.block_timestamp
                    )
                )
            )
            OR t0.block_number IN (
              SELECT
                DISTINCT t4.block_number AS block_number
              FROM
                {{ this }}
                t4
              WHERE
                t4.amount_out_usd IS NULL
                AND t4._inserted_timestamp < (
                  SELECT
                    MAX(
                      _inserted_timestamp
                    ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
                  FROM
                    {{ this }}
                )
                AND EXISTS (
                  SELECT
                    1
                  FROM
                    {{ ref('silver__complete_token_prices') }}
                    p
                  WHERE
                    p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND p.price IS NOT NULL
                    AND p.token_address = t4.token_out
                    AND p.hour = DATE_TRUNC(
                      'hour',
                      t4.block_timestamp
                    )
                )
            )
        ),
      {% endif %}

      FINAL AS (
        SELECT
          *
        FROM
          complete_dex_swaps_t2

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  *
FROM
  heal_model
{% endif %}
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  token0_address,
  token1_address,
  amount0_unadj,
  amount1_unadj,
  amount0_adjusted,
  amount1_adjusted,
  amount0_usd,
  amount1_usd,
  amount_in_unadj,
  amount_in,
  amount_in_usd,
  amount_out_unadj,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  version,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_dex_swaps_t2_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify (ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
