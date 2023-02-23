{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'blocks', 'sql_limit', {{var('sql_limit','1000')}}, 'producer_batch_size', {{var('producer_batch_size','1000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','100')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(1) %}
    (

        SELECT
            PARSE_JSON(
                CONCAT(
                    '{"method": "eth_getBlockByNumber", "params":[',
                    block_number :: STRING,
                    ',',
                    false,
                    '],"id":',
                    block_number :: STRING,
                    '}'
                )
            ) AS request
        FROM
            {{ ref("streamline__blocks") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            block_number
        FROM
            {{ ref("streamline__complete_blocks") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
