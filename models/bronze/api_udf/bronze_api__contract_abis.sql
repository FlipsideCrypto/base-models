{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = false
) }}

WITH api_keys AS (

    SELECT
        api_key
    FROM
        {{ source(
            'silver_crosschain',
            'apis_keys'
        ) }}
    WHERE
        api_name = 'basescan'
),
base AS (
    SELECT
        contract_address
    FROM
        {{ ref('silver__relevant_abi_contracts') }}

{% if is_incremental() %}
EXCEPT
SELECT
    contract_address
FROM
    {{ this }}
WHERE
    abi_data :data :result :: STRING <> 'Max rate limit reached'
{% endif %}
LIMIT
    5
), row_nos AS (
    SELECT
        contract_address,
        ROW_NUMBER() over (
            ORDER BY
                contract_address
        ) AS row_no,
        CEIL(
            row_no / 1
        ) AS batch_no,
        api_key
    FROM
        base
        JOIN api_keys
        ON 1 = 1
),
batched AS ({% for item in range(6) %}
SELECT
    rn.contract_address, ethereum.streamline.udf_api('GET', CONCAT('https://api.basescan.org/api?module=contract&action=getabi&address=', rn.contract_address, '&apikey=', api_key),{ 'User-Agent': 'FlipsideStreamline' },{}) AS abi_data, SYSDATE() AS _inserted_timestamp
FROM
    row_nos rn
WHERE
    batch_no = {{ item }}
    AND EXISTS (
SELECT
    1
FROM
    row_nos
WHERE
    batch_no = {{ item }}
LIMIT
    1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    contract_address,
    abi_data,
    _inserted_timestamp
FROM
    batched
