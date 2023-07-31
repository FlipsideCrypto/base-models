{{ config (
    materialized = "incremental",
    unique_key = "_call_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["_call_id"],
    tags = ['base_goerli']
) }}

WITH new_txs AS (

    SELECT
        block_number,
        tx_hash,
        response,
        _inserted_timestamp
    FROM
        {{ ref('silver_goerli__traces_method') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
        {% endif %}
    ),
    base_table AS (
        SELECT
            CASE
                WHEN POSITION(
                    '.',
                    path :: STRING
                ) > 0 THEN REPLACE(
                    REPLACE(
                        path :: STRING,
                        SUBSTR(path :: STRING, len(path :: STRING) - POSITION('.', REVERSE(path :: STRING)) + 1, POSITION('.', REVERSE(path :: STRING))),
                        ''
                    ),
                    '.',
                    '__'
                )
                ELSE '__'
            END AS id,
            OBJECT_AGG(
                DISTINCT key,
                VALUE
            ) AS DATA,
            tx_hash,
            block_number,
            _inserted_timestamp
        FROM
            new_txs txs,
            TABLE(
                FLATTEN(
                    input => PARSE_JSON(
                        txs.response
                    ),
                    recursive => TRUE
                )
            ) f
        WHERE
            f.index IS NULL
            AND f.key != 'calls'
        GROUP BY
            tx_hash,
            id,
            block_number,
            _inserted_timestamp
    ),
    flattened_traces AS (
        SELECT
            DATA :from :: STRING AS from_address,
            utils.udf_hex_to_int(
                DATA :gas :: STRING
            ) AS gas,
            utils.udf_hex_to_int(
                DATA :gasUsed :: STRING
            ) AS gas_used,
            DATA :input :: STRING AS input,
            DATA :output :: STRING AS output,
            DATA :to :: STRING AS to_address,
            DATA :type :: STRING AS TYPE,
            CASE
                WHEN DATA :type :: STRING = 'CALL' THEN utils.udf_hex_to_int(
                    DATA :value :: STRING
                ) / pow(
                    10,
                    18
                )
                ELSE 0
            END AS eth_value,
            CASE
                WHEN id = '__' THEN CONCAT(
                    DATA :type :: STRING,
                    '_ORIGIN'
                )
                ELSE CONCAT(
                    DATA :type :: STRING,
                    '_',
                    REPLACE(
                        REPLACE(REPLACE(REPLACE(id, 'calls', ''), '[', ''), ']', ''),
                        '__',
                        '_'
                    )
                )
            END AS identifier,
            concat_ws(
                '-',
                tx_hash,
                identifier
            ) AS _call_id,
            SPLIT(
                identifier,
                '_'
            ) AS id_split,
            ARRAY_SLICE(id_split, 1, ARRAY_SIZE(id_split)) AS levels,
            ARRAY_TO_STRING(
                levels,
                '_'
            ) AS LEVEL,
            CASE
                WHEN ARRAY_SIZE(levels) = 1
                AND levels [0] :: STRING = 'ORIGIN' THEN NULL
                WHEN ARRAY_SIZE(levels) = 1 THEN 'ORIGIN'
                ELSE ARRAY_TO_STRING(ARRAY_SLICE(levels, 0, ARRAY_SIZE(levels) -1), '_')END AS parent_level,
                COUNT(parent_level) over (
                    PARTITION BY tx_hash,
                    parent_level
                ) AS sub_traces,*
                FROM
                    base_table
            ),
            group_sub_traces AS (
                SELECT
                    tx_hash,
                    parent_level,
                    sub_traces
                FROM
                    flattened_traces
                GROUP BY
                    tx_hash,
                    parent_level,
                    sub_traces
            ),
            FINAL AS (
                SELECT
                    flattened_traces.tx_hash AS tx_hash,
                    flattened_traces.block_number AS block_number,
                    flattened_traces.from_address AS from_address,
                    flattened_traces.to_address AS to_address,
                    flattened_traces.eth_value AS eth_value,
                    COALESCE(
                        flattened_traces.gas,
                        0
                    ) AS gas,
                    COALESCE(
                        flattened_traces.gas_used,
                        0
                    ) AS gas_used,
                    flattened_traces.input AS input,
                    flattened_traces.output AS output,
                    flattened_traces.type AS TYPE,
                    flattened_traces.identifier AS identifier,
                    flattened_traces._call_id AS _call_id,
                    flattened_traces.data AS DATA,
                    flattened_traces._inserted_timestamp AS _inserted_timestamp,
                    group_sub_traces.sub_traces AS sub_traces
                FROM
                    flattened_traces
                    LEFT OUTER JOIN group_sub_traces
                    ON flattened_traces.tx_hash = group_sub_traces.tx_hash
                    AND flattened_traces.level = group_sub_traces.parent_level
            )
        SELECT
            f.tx_hash,
            f.block_number,
            t.block_timestamp,
            f.from_address,
            f.to_address,
            f.eth_value,
            f.gas,
            f.gas_used,
            f.input,
            f.output,
            f.type,
            f.identifier,
            f._call_id,
            f.data,
            t.tx_status,
            f.sub_traces,
            f._inserted_timestamp
        FROM
            FINAL f
            JOIN {{ ref('silver_goerli__transactions') }}
            t
            ON f.tx_hash = t.tx_hash
        WHERE
            identifier IS NOT NULL

{% if is_incremental() %}
AND t._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY _call_id
ORDER BY
    f._inserted_timestamp DESC)) = 1
