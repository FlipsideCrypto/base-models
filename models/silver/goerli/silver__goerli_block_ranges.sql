{{ config (
    materialized = "table",
    unique_key = "batch_id"
) }}

WITH inputs AS (

    SELECT
        8492014 AS min_block
),
chainhead AS (
    SELECT
        udfs.streamline.udf_call_node(
            secret_name,
            node_url,
            headers,
            [ { 'jsonrpc': '2.0', 'method': 'eth_blockNumber', 'params': [],
            'id': 1 }]
        ) AS resp,
        chain
    FROM
        {{ source(
            'udfs_streamline',
            'node_mapping'
        ) }}
    WHERE
        chain = 'ethereum-goerli'
),
ranges AS (
    SELECT
        ethereum.public.udf_hex_to_int(
            resp :data [0] :result :: STRING
        ) :: INTEGER AS chainhead,
        min_block
    FROM
        chainhead
        JOIN inputs
        ON 1 = 1
),
generate_range AS (
    SELECT
        SEQ4() AS block_number
    FROM
        TABLE(GENERATOR(rowcount => 50000000))
),
blocks AS (
    SELECT
        block_number
    FROM
        generate_range
        JOIN ranges
        ON block_number BETWEEN min_block
        AND chainhead
),
batched AS (
    SELECT
        block_number,
        ROW_NUMBER() over (
            ORDER BY
                block_number
        ) AS id,
        FLOOR(
            id / 100
        ) AS batch_id
    FROM
        blocks
),
grouped AS (
    SELECT
        batch_id + 1 AS batch_id,
        MIN(block_number) AS min_b,
        MAX(block_number) AS max_b
    FROM
        batched
    GROUP BY
        batch_id
)
SELECT
    batch_id,
    min_b,
    max_b,
    CONCAT('0x', TRIM(to_char(min_b, 'XXXXXXXX'))) AS fromBlock,
    CONCAT('0x', TRIM(to_char(max_b, 'XXXXXXXX'))) AS toBlock
FROM
    grouped
