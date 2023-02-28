 {{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["_log_id"]
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "qn_getBlockWithReceipts") }}'
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
        DATA :result :receipts AS response,
        registered_on AS _inserted_timestamp
    FROM
        {{ source(
            "bronze_streamline",
            "qn_getBlockWithReceipts"
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
),

flat_response AS (

SELECT
    block_number,
    VALUE :blockHash :: STRING AS blockHash,
	VALUE :cumulativeGasUsed :: STRING AS cumulativeGasUsed,
    VALUE :effectiveGasPrice :: STRING AS effectiveGasPrice,
    VALUE :from :: STRING AS origin_from_address,
    VALUE :gasUsed :: STRING AS gasUsed,
    VALUE :logs AS logs_array,
    VALUE :logsBloom :: STRING AS logsBloom,
    VALUE :status :: STRING AS status,
    VALUE :to :: STRING AS origin_to_address,
    VALUE :type :: STRING AS type,
    response,
    _inserted_timestamp
FROM
    base,
    LATERAL FLATTEN(input => response)
)

SELECT
	block_number,
    blockHash,
    cumulativeGasUsed,
    effectiveGasPrice,
    origin_from_address,
    gasUsed,
    logs_array,
    VALUE :address :: STRING AS contract_address,
    VALUE :data :: STRING AS data,
    ethereum.public.udf_hex_to_int(
    	VALUE :logIndex :: STRING) :: INTEGER AS log_index,
    VALUE :removed :: STRING AS removed,
    VALUE :topics AS topics,
    VALUE :transactionHash :: STRING AS tx_hash,
    VALUE :transactionIndex :: STRING AS transactionIndex,
    logsBloom,
    status,
    origin_to_address,
    type,
    CONCAT(
        tx_hash,
        '-',
        log_index
    ) AS _log_id,
    response,
    _inserted_timestamp
FROM flat_response,
    LATERAL FLATTEN(input => logs_array)
