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
                table_name => '{{ source( "bronze_streamline", "eth_getTransactionReceipt") }}'
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
            "eth_getTransactionReceipt"
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
    response :blockHash :: STRING AS blockHash,
    response :transactionHash :: STRING AS parent_transactionHash,
    ethereum.public.udf_hex_to_int(
    	response :transactionIndex :: STRING) :: INTEGER AS parent_transactionIndex,
	response :cumulativeGasUsed :: STRING AS cumulativeGasUsed,
    response :effectiveGasPrice :: STRING AS effectiveGasPrice,
    response :gasUsed :: STRING AS gasUsed,
    response :l1Fee :: STRING AS l1Fee,
    response :l1FeeScalar :: STRING AS l1FeeScalar,
    response :l1GasUsed :: STRING AS l1GasUsed,
    response :l1GasPrice :: STRING AS l1GasPrice,
    response :logs AS logs_array,
    response :logsBloom :: STRING AS logsBloom,
    response :status :: STRING AS status,
    response :from :: STRING AS origin_from_address,
    response :to :: STRING AS origin_to_address,
    response :type :: STRING AS type,
    response,
    _inserted_timestamp
FROM
    base
),

logs_response AS (

SELECT
	block_number,
    parent_transactionHash,
    parent_transactionIndex,
    logs_array,
	VALUE :address :: STRING AS contract_address,
    VALUE :data :: STRING AS data,
    ethereum.public.udf_hex_to_int(
    	VALUE :logIndex :: STRING) :: INTEGER AS event_index,
    VALUE :removed :: STRING AS removed,
    VALUE :topics AS topics,
    VALUE :transactionHash :: STRING AS tx_hash,
    VALUE :transactionIndex :: STRING AS transactionIndex,
    _inserted_timestamp
FROM flat_response,
    LATERAL FLATTEN(input => logs_array)
)

SELECT
    block_number,
    blockHash,
    parent_transactionHash,
    parent_transactionIndex,
    cumulativeGasUsed,
    effectiveGasPrice,
    gasUsed,
    l1Fee,
    l1FeeScalar,
    l1GasUsed,
    l1GasPrice,
    l.logs_array,
    contract_address,
    data,
    event_index,
    removed,
    topics,
    tx_hash,
    transactionIndex,
    logsBloom,
    status,
    origin_from_address,
    origin_to_address,
    type,
    CONCAT(
        COALESCE(tx_hash,parent_transactionHash),
        '-',
        COALESCE(event_index,parent_transactionIndex)
    ) AS _log_id,
    response,
    _inserted_timestamp
FROM flat_response f
LEFT JOIN logs_response l USING(parent_transactionHash)
