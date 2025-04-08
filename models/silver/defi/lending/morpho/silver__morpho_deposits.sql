{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH traces AS (

    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        from_address,
        to_address,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_input,
        CONCAT('0x', SUBSTR(segmented_input [0] :: STRING, 25)) AS loan_token,
        CONCAT('0x', SUBSTR(segmented_input [1] :: STRING, 25)) AS collateral_token,
        CONCAT('0x', SUBSTR(segmented_input [2] :: STRING, 25)) AS oracle_address,
        CONCAT('0x', SUBSTR(segmented_input [3] :: STRING, 25)) AS irm_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [4] :: STRING
            )
        ) AS lltv,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [5] :: STRING
            )
        ) AS amount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [6] :: STRING
            )
        ) AS shares,
        CONCAT('0x', SUBSTR(segmented_input [7] :: STRING, 25)) AS on_behalf_address,
        CONCAT('0x', SUBSTR(segmented_input [8] :: STRING, 25)) AS receiver_address,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature
    FROM
        {{ ref('core__fact_traces') }}
        t
    WHERE
        to_address = '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb' --Morpho Blue
        AND function_sig = '0xa99aad89'
        AND trace_succeeded
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
tx_join AS (
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        from_address,
        to_address AS contract_address,
        origin_from_address AS depositor_address,
        loan_token,
        collateral_token,
        amount,
        on_behalf_address,
        receiver_address,
        _call_id,
        _inserted_timestamp
    FROM
        traces
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    t.contract_address,
    loan_token AS market,
    amount AS amount_unadj,
    amount / pow(
        10,
        C.token_decimals
    ) AS amount,
    C.token_symbol,
    C.token_decimals,
    depositor_address,
    t.contract_address AS lending_pool_contract,
    'Morpho Blue' AS platform,
    'base' AS blockchain,
    _call_id AS _id,
    t._inserted_timestamp
FROM
    tx_join t
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON C.contract_address = t.loan_token
