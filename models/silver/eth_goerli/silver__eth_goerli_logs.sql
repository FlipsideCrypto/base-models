{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['round(block_number,-3)']
) }}

WITH base AS (

    SELECT
        resp,
        resp :data [0] :result AS RESULT,
        _inserted_timestamp
    FROM
        {{ ref('bronze__eth_goerli_contract_logs') }}
    WHERE
        resp :data [0] :result :: STRING <> '[]'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    VALUE :address :: STRING AS contract_address,
    VALUE :blockHash :: STRING AS block_hash,
    utils.udf_hex_to_int(
        VALUE :blockNumber :: STRING
    ) :: INTEGER AS block_number,
    VALUE :data :: STRING AS DATA,
    utils.udf_hex_to_int(
        VALUE :logIndex :: STRING
    ) :: INTEGER AS log_index,
    CASE
        WHEN VALUE :removed :: STRING = 'true' THEN TRUE
        ELSE FALSE
    END AS removed,
    VALUE :topics AS topics,
    VALUE :transactionHash :: STRING AS tx_hash,
    utils.udf_hex_to_int(
        VALUE :transactionIndex :: STRING
    ) :: INTEGER AS tx_index,
    CONCAT(
        tx_hash,
        '-',
        log_index
    ) AS _log_id,
    _inserted_timestamp
FROM
    base,
    LATERAL FLATTEN(
        input => RESULT
    ) qualify ROW_NUMBER() over (
        PARTITION BY _log_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
