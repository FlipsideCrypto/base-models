{{ config (
    materialized = "incremental",
    unique_key = "ID",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["ID"]
) }}

WITH meta AS (

    SELECT
        registered_on AS _inserted_timestamp,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline_goerli", "debug_traceTransaction") }}'
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
{% endif %}
SELECT
    DATA :result AS response,
    SPLIT(
        DATA :id :: STRING,
        '-'
    ) AS split_id,
    split_id [0] :: INT AS block_number,
    split_id [1] :: STRING AS tx_hash,
    DATA :id :: STRING AS id,
    _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline_goerli",
        "debug_traceTransaction"
    ) }}
    t
    JOIN meta b
    ON b.file_name = metadata$filename

{% if is_incremental() %}
JOIN partitions p
ON p._partition_by_block_number = t._partition_by_block_id
{% endif %}
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
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
