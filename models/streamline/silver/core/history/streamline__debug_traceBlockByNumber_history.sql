{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_traces(object_construct('sql_source', '{{this.identifier}}','exploded_key','[\"result\"]', 'method', 'debug_traceBlockByNumber', 'external_table', 'debug_traceBlockByNumber', 'sql_limit', {{var('sql_limit','200000')}}, 'producer_batch_size', {{var('producer_batch_size','200000')}}, 'worker_batch_size', {{var('worker_batch_size','100000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_history']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
blocks AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number <= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_debug_traceBlockByNumber") }}
    WHERE
        block_number <= (
            SELECT
                block_number
            FROM
                last_3_days
        )
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "debug_traceBlockByNumber", "params":["',
            REPLACE(
                concat_ws(
                    '',
                    '0x',
                    to_char(
                        block_number :: INTEGER,
                        'XXXXXXXX'
                    )
                ),
                ' ',
                ''
            ),
            '",{"tracer": "callTracer","timeout": "30s"}',
            '],"id":"',
            block_number :: INTEGER,
            '"}'
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_number ASC
