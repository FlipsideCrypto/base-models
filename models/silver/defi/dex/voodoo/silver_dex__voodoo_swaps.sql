{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH swaps_base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS account_address,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [1] :: STRING,
                25,
                40
            )
        ) AS tokenIn,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [2] :: STRING,
                25,
                40
            )
        ) AS tokenOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [4] :: STRING
            )
        ) AS amountOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [5] :: STRING
            )
        ) AS amountOutAfterFees,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [6] :: STRING
            )
        ) AS feeBasisPoints,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x4f188afdc40e6d2ddddf5fd1b2df7aef7da52f50' --Vault
        AND topics [0] :: STRING = '0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db' --Swap
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
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    origin_from_address AS sender,
    account_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    amountOutAfterFees AS amount_out_after_fees,
    feeBasisPoints AS fee_basis_points,
    'Swap' AS event_name,
    'voodoo' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
