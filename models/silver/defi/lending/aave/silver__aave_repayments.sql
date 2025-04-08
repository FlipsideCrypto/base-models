{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH repay AS(

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS aave_market,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS repayer,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repayed_amount,
        CASE
            WHEN contract_address = LOWER('0xa238dd80c259a72e81d7e4664a9801593f98d1c5') THEN 'Aave V3'
            ELSE 'ERROR'
        END AS aave_version,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        origin_from_address AS repayer_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
AND contract_address = LOWER('0xa238dd80c259a72e81d7e4664a9801593f98d1c5')
AND tx_succeeded
),
atoken_meta AS (
    SELECT
        atoken_address,
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
        {{ ref('silver__aave_tokens') }}
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
    aave_market,
    atoken_meta.atoken_address AS aave_token,
    repayed_amount AS amount_unadj,
    repayed_amount / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS amount,
    repayer_address AS payer,
    borrower_address AS borrower,
    lending_pool_contract,
    aave_version AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'base' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    repay
    LEFT JOIN atoken_meta
    ON repay.aave_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
