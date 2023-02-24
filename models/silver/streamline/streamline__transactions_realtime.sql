{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transactions', 'sql_limit', {{var('sql_limit','900')}}, 'producer_batch_size', {{var('producer_batch_size','300')}}, 'worker_batch_size', {{var('worker_batch_size','300')}}, 'batch_call_limit', {{var('batch_call_limit','30')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    PARSE_JSON(
        CONCAT(
            '{"method": "eth_getBlockByNumber", "params":[',
            block_number :: STRING,
            ',',
            TRUE :: BOOLEAN,
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
    {{ ref("streamline__complete_blocks") }}
WHERE
    block_number > 1000000
