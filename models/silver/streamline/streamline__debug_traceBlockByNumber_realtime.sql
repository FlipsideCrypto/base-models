{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'debug_traceBlockByNumber', 'sql_limit', {{var('sql_limit','6000')}}, 'producer_batch_size', {{var('producer_batch_size','6000')}}, 'worker_batch_size', {{var('worker_batch_size','3000')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_number :: STRING AS block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number > 1000000
    EXCEPT
    SELECT
        block_number :: STRING
    FROM
        {{ ref("streamline__complete_debug_traceBlockByNumber") }}
    WHERE
        block_number > 1000000
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "debug_traceBlockByNumber", "params":[',
            block_number :: STRING,
            ',{"tracer": "callTracer"}',
            '],"id":',
            block_number :: STRING,
            '}'
        )
    ) AS request
FROM
    blocks
