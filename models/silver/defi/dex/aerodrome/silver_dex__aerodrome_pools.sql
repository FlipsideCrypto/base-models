{{ config(
    materialized = 'incremental',
    unique_key = "pool_address",
    tags = ['non_realtime']
) }}

with created_pools as(
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1,
        LOWER(CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))) AS stable,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: INTEGER AS pool_number,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        _log_id,
        _inserted_timestamp
    from
        base_dev.silver.logs
    where topics[0] = '0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e'
        and contract_address = '0x420dd381b31aef6683db6b902084cb0ffece40da'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
AND pool_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    segmented_data,
    token0,
    token1,
    CASE
        WHEN stable = '0x0000000000000000000000000000000000000001' THEN TRUE
        WHEN stable = '0x0000000000000000000000000000000000000000' THEN FALSE
    END AS stable,
    pool_number,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM 
    created_pools