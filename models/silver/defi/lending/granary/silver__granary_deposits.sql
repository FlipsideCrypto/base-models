{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curated']
) }}

WITH 
atoken_meta AS (
    SELECT
        atoken_address,
        granary_version_pool,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__granary_tokens') }}
),
deposits AS(

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS granary_market,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS onBehalfOf,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER AS refferal,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS userAddress,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS deposit_quantity,
        'Granary' AS granary_version,
        origin_from_address AS depositor_address,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xde6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951'

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
AND contract_address in (SELECT granary_version_pool from atoken_meta)
AND tx_status = 'SUCCESS' --excludes failed txs
)

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    granary_market,
    atoken_meta.atoken_address AS granary_token,
    deposit_quantity AS amount_unadj,
    deposit_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS amount,
    depositor_address,
    lending_pool_contract,
    granary_version AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'base' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    deposits
    LEFT JOIN atoken_meta
    ON deposits.granary_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
