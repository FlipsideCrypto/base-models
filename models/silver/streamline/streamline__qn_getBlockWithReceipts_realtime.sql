{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'qn_getBlockWithReceipts', 'sql_limit', {{var('sql_limit','300000')}}, 'producer_batch_size', {{var('producer_batch_size','60000')}}, 'worker_batch_size', {{var('worker_batch_size','30000')}}, 'batch_call_limit', {{var('batch_call_limit','200')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "qn_getBlockWithReceipts", "params":[',
            block_number :: STRING,
            '],"id":',
            block_number :: STRING,
            '}'
        )
    ) AS request
FROM
    {{ ref("streamline__blocks") }}
WHERE
    block_number > 1000000
    AND block_number IS NOT NULL
EXCEPT
SELECT
    block_number
FROM
    {{ ref("streamline__complete_qn_getBlockWithReceipts") }}
WHERE
    block_number > 1000000
