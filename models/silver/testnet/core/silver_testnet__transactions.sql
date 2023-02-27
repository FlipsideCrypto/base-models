{{ config (
    materialized = "incremental",
    unique_key = "tx_hash",
    cluster_by = "BLOCK_TIMESTAMP::DATE",
    merge_update_columns = ["tx_hash"]
) }}

SELECT
    block_number,
    TO_TIMESTAMP_NTZ(
        ethereum.public.udf_hex_to_int(
            block_timestamp :: STRING
        )
    ) AS block_timestamp,
    tx_hash,
    ethereum.public.udf_hex_to_int(
        nonce :: STRING
    ) :: INTEGER AS nonce,
    ethereum.public.udf_hex_to_int(
        POSITION :: STRING
    ) :: INTEGER AS POSITION,
    SUBSTR(
        input,
        1,
        10
    ) AS origin_function_signature,
    from_address,
    to_address,
    ethereum.public.udf_hex_to_int(
        eth_value :: STRING
    ) / pow(
        10,
        18
    ) AS eth_value,
    block_hash,
    COALESCE(
        ethereum.public.udf_hex_to_int(
            gas_price :: STRING
        ) / pow(
            10,
            9
        ),
        0
    ) AS gas_price,
    ethereum.public.udf_hex_to_int(
        gas_limit :: STRING
    ) :: INTEGER AS gas_limit,
    input AS input_data,
    -- need receipts for tx status, gas used, L1 gas prices
    ethereum.public.udf_hex_to_int(
        tx_type :: STRING
    ) :: INTEGER AS tx_type,
    is_system_tx,
    object_construct_keep_null(
        'chain_ID',
        ethereum.public.udf_hex_to_int(
            chainID :: STRING
        ) :: INTEGER,
        'r',
        r,
        's',
        s,
        'v',
        ethereum.public.udf_hex_to_int(
            v :: STRING
        ) :: INTEGER,
        'access_list',
        accesslist,
        'max_priority_fee_per_gas',
        ethereum.public.udf_hex_to_int(
            max_priority_fee_per_gas :: STRING
        ) :: INTEGER,
        'max_fee_per_gas',
        ethereum.public.udf_hex_to_int(
            max_fee_per_gas :: STRING
        ),
        'mint',
        ethereum.public.udf_hex_to_int(
            mint :: STRING
        ),
        'source_hash',
        sourcehash
    ) AS tx_json,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver_testnet__tx_method') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
