{{ config (
    materialized = "incremental",
    unique_key = "tx_hash",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["tx_hash"]
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "goerli_transactions") }}'
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
        DATA :result AS block_response,
        DATA :result :transactions AS tx_response,
        registered_on AS _inserted_timestamp
    FROM
        {{ source(
            "bronze_streamline",
            "goerli_transactions"
        ) }}
        t
        JOIN meta b
        ON b.file_name = metadata$filename --needs better partitioning once Ryan fixes his version of the model
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
flat AS (
    SELECT
        block_number,
        block_response :timestamp :: STRING AS block_timestamp,
        VALUE :hash :: STRING AS tx_hash,
        VALUE :blockHash :: STRING AS block_hash,
        VALUE :blockNumber :: STRING AS blockNumber,
        VALUE :chainId :: STRING AS chainId,
        VALUE :from :: STRING AS from_address,
        VALUE :gas :: STRING AS gas_limit,
        VALUE :gasPrice :: STRING AS gas_price,
        VALUE :input :: STRING AS input,
        CASE
            WHEN VALUE :isSystemTx :: STRING = 'true' THEN TRUE
            ELSE FALSE
        END AS is_system_tx,
        VALUE :maxFeePerGas :: STRING AS max_fee_per_gas,
        VALUE :mint :: STRING AS mint,
        VALUE :maxPriorityFeePerGas :: STRING AS max_priority_fee_per_gas,
        VALUE :nonce :: STRING AS nonce,
        VALUE :r :: STRING AS r,
        VALUE :s :: STRING AS s,
        VALUE :sourceHash :: STRING AS sourceHash,
        VALUE :to :: STRING AS to_address,
        VALUE :transactionIndex :: STRING POSITION,
        VALUE :type :: STRING AS tx_type,
        VALUE :v :: STRING AS v,
        VALUE :value :: STRING AS eth_value,
        VALUE :accessList AS accessList,
        VALUE,
        block_response,
        _inserted_timestamp
    FROM
        base,
        LATERAL FLATTEN (
            input => tx_response
        )
)
SELECT
    *
FROM
    flat qualify(ROW_NUMBER() over (PARTITION BY tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1
