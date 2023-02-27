{{ config(
    materialized = 'incremental',
    unique_key = 'batch_id',
    full_refresh = False
) }}

WITH request_details AS (

    SELECT
        batch_id,
        fromBlock,
        toBlock,
        contract_address,
        ROW_NUMBER() over (
            ORDER BY
                batch_id
        ) AS batch_no
    FROM
        {{ ref('silver__goerli_contract_reads') }}

{% if is_incremental() %}
WHERE
    batch_id NOT IN (
        SELECT
            DISTINCT batch_id
        FROM
            {{ this }}
    )
{% endif %}
ORDER BY
    batch_id ASC
LIMIT
    10
), ready_requests AS (
    SELECT
        CONCAT(
            '[{\'id\': 1, \'jsonrpc\': \'2.0\', \'method\': \'eth_getLogs\',\'params\': [{ \'address\': \'',
            contract_address,
            '\' ,\'fromBlock\': \'',
            fromBlock,
            '\',',
            '\'toBlock\': \'',
            toBlock,
            '\'}]}]'
        ) AS json_request,
        batch_id,
        batch_no
    FROM
        request_details
),
batched AS ({% for item in range(10) %}
SELECT
    udfs.streamline.udf_call_node(secret_name, node_url, headers, PARSE_JSON(json_request)) AS resp, batch_id, SYSDATE() AS _inserted_timestamp
FROM
    ready_requests
    JOIN {{ source('udfs_streamline', 'node_mapping') }}
    ON 1 = 1
WHERE
    batch_no = {{ item }} + 1
    AND chain = 'ethereum-goerli'
    AND EXISTS (
SELECT
    1
FROM
    request_details
LIMIT
    1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    batch_id,
    resp,
    _inserted_timestamp
FROM
    batched
