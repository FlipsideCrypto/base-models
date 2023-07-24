 {{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["block_number"]
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "goerli_blocks") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    partitions AS (
        SELECT
            DISTINCT CAST(
                SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER
            ) AS _partition_by_block_number
        FROM
            meta
    )
{% else %}
)
{% endif %},
base AS (
    SELECT
        block_number,
        DATA :result AS response,
        registered_on AS _inserted_timestamp
    FROM
        {{ source(
            "bronze_streamline",
            "goerli_blocks"
        ) }}
        t
        JOIN meta b
        ON b.file_name = metadata$filename -- add better partitioning
    WHERE
        DATA :error :code IS NULL
        OR DATA :error :code NOT IN (
            '-32000',
            '-32001',
            '-32002',
            '-32003',
            '-32004',
            '-32005',
            '-32006',
            '-32007',
            '-32008',
            '-32009',
            '-32010'
        ) qualify(ROW_NUMBER() over (PARTITION BY block_number
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    block_number,
    response :baseFeePerGas :: STRING AS baseFeePerGas,
    response :difficulty :: STRING AS difficulty,
    response :extraData :: STRING AS extraData,
    response :gasLimit :: STRING AS gasLimit,
    response :gasUsed :: STRING AS gasUsed,
    response :hash :: STRING AS block_hash,
    response :logsBloom :: STRING AS logsBloom,
    response :miner :: STRING AS miner,
    response :mixHash :: STRING AS mixHash,
    response :nonce :: STRING AS nonce,
    response :number :: STRING AS NUMBER,
    response :parentHash :: STRING AS parentHash,
    response :receiptsRoot :: STRING AS receiptsRoot,
    response :sha3Uncles :: STRING AS sha3Uncles,
    response :size :: STRING AS SIZE,
    response :stateRoot :: STRING AS stateRoot,
    response :timestamp :: STRING TIMESTAMP,
    response :totalDifficulty :: STRING AS totalDifficulty,
    response :transactions AS transactions,
    ARRAY_SIZE(
        response :transactions
    ) AS tx_count,
    response :transactionsRoot :: STRING AS transactionsRoot,
    response :uncles AS uncles,
    response,
    _inserted_timestamp
FROM
    base
