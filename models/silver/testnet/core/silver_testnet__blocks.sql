{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "block_timestamp::date",
    merge_update_columns = ["block_number"]
) }}

SELECT
    block_number,
    TO_TIMESTAMP_NTZ(
        ethereum.public.udf_hex_to_int(
            TIMESTAMP :: STRING
        )
    ) AS block_timestamp,
    'testnet' AS network,
    'base' AS blockchain,
    tx_count,
    ethereum.public.udf_hex_to_int(
        difficulty :: STRING
    ) :: INTEGER AS difficulty,
    ethereum.public.udf_hex_to_int(
        totalDifficulty :: STRING
    ) :: INTEGER AS total_difficulty,
    extraData AS extra_data,
    ethereum.public.udf_hex_to_int(
        gasLimit :: STRING
    ) :: INTEGER AS gas_limit,
    ethereum.public.udf_hex_to_int(
        gasUsed :: STRING
    ) :: INTEGER AS gas_used,
    block_hash AS HASH,
    parentHash AS parent_hash,
    receiptsRoot AS receipts_root,
    sha3Uncles AS sha3_uncles,
    ethereum.public.udf_hex_to_int(
        SIZE :: STRING
    ) :: INTEGER AS SIZE,
    uncles AS uncle_blocks,
    object_construct_keep_null(
        'transactions',
        transactions,
        'transactions_root',
        transactionsRoot,
        'logs_bloom',
        logsBloom,
        'miner',
        miner,
        'mix_hash',
        mixHash,
        'nonce',
        ethereum.public.udf_hex_to_int(
            nonce :: STRING
        ) :: INTEGER
    ) AS block_header_json,
    _inserted_timestamp
FROM
    {{ ref('silver_testnet__blocks_method') }}

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
