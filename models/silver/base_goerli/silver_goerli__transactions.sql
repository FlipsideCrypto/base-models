{{ config (
    materialized = "incremental",
    unique_key = "tx_hash",
    cluster_by = "BLOCK_TIMESTAMP::DATE"
) }}

WITH flat_base AS (

    SELECT
        t.block_number,
        TO_TIMESTAMP_NTZ(
            ethereum.public.udf_hex_to_int(
                block_timestamp :: STRING
            )
        ) AS block_timestamp,
        t.tx_hash,
        ethereum.public.udf_hex_to_int(
            nonce :: STRING
        ) :: INTEGER AS nonce,
        ethereum.public.udf_hex_to_int(
            POSITION :: STRING
        ) :: INTEGER AS POSITION,
        SUBSTR(
            input,
            1,
            10
        ) AS origin_function_signature,
        from_address,
        to_address,
        COALESCE(
            ethereum.public.udf_hex_to_int(
                eth_value :: STRING
            ) :: INTEGER / pow(
                10,
                18
            ),
            0
        ) AS eth_value,
        block_hash,
        COALESCE(
            ethereum.public.udf_hex_to_int(
                gas_price :: STRING
            ) :: INTEGER,
            0
        ) AS gas_price1,
        ethereum.public.udf_hex_to_int(
            gas_limit :: STRING
        ) :: INTEGER AS gas_limit,
        input AS input_data,
        ethereum.public.udf_hex_to_int(
            tx_type :: STRING
        ) :: INTEGER AS tx_type,
        is_system_tx,
        object_construct_keep_null(
            'chain_ID',
            ethereum.public.udf_hex_to_int(
                chainID :: STRING
            ) :: INTEGER,
            'r',
            r,
            's',
            s,
            'v',
            ethereum.public.udf_hex_to_int(
                v :: STRING
            ) :: INTEGER,
            'access_list',
            accesslist,
            'max_priority_fee_per_gas',
            COALESCE(
                ethereum.public.udf_hex_to_int(
                    max_priority_fee_per_gas :: STRING
                ) :: INTEGER,
                0
            ),
            'max_fee_per_gas',
            COALESCE(
                ethereum.public.udf_hex_to_int(
                    max_fee_per_gas :: STRING
                ) :: INTEGER,
                0
            ),
            'mint',
            ethereum.public.udf_hex_to_int(
                mint :: STRING
            ),
            'source_hash',
            sourcehash
        ) AS tx_json,
        CASE
            WHEN status = '0x1' THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        COALESCE(ethereum.public.udf_hex_to_int(gasUsed :: STRING) :: INTEGER, 0) AS gas_used,
        COALESCE(
            ethereum.public.udf_hex_to_int(
                cumulativeGasUsed :: STRING
            ) :: INTEGER,
            0
        ) AS cumulative_gas_used,
        COALESCE(
            ethereum.public.udf_hex_to_int(
                effectiveGasPrice
            ) :: INTEGER,
            0
        ) AS effective_gas_price,
        COALESCE(
            ethereum.public.udf_hex_to_int(
                l1FeeScalar :: STRING
            ) :: FLOAT,
            0
        ) AS l1_fee_scalar,
        COALESCE(
            ethereum.public.udf_hex_to_int(
                l1GasUsed :: STRING
            ) :: FLOAT,
            0
        ) AS l1_gas_used,
        COALESCE(
            ethereum.public.udf_hex_to_int(
                l1GasPrice :: STRING
            ) :: FLOAT,
            0
        ) AS l1_gas_price,
        COALESCE(
            ((gas_used * gas_price1) + (l1_gas_price * l1_gas_used * l1_fee_scalar)) / pow(
                10,
                18
            ),
            0
        ) AS tx_fee,
        t._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_goerli__tx_method') }}
        t
        JOIN {{ ref('silver_goerli__receipts_method') }}
        l
        ON t.tx_hash = l.tx_hash

{% if is_incremental() %}
WHERE
    t._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
    )
{% endif %}
),

new_records AS (

SELECT
    f.block_number,
    b.block_timestamp,
    f.tx_hash,
    f.nonce,
    f.POSITION,
    f.from_address,
    f.to_address,
    f.eth_value,
    f.block_hash,
    f.gas_price1 / pow(
        10,
        9
    ) AS gas_price,
    f.gas_limit,
    f.input_data,
    f.tx_type,
    f.is_system_tx,
    f.tx_json,
    f.tx_status,
    f.gas_used,
    f.cumulative_gas_used,
    f.effective_gas_price,
    f.l1_fee_scalar,
    f.l1_gas_used,
    f.l1_gas_price / pow(
        10,
        9
    ) AS l1_gas_price,
    f.tx_fee,
    f.origin_function_signature,
    CASE
        WHEN b.block_timestamp IS NULL THEN TRUE
        ELSE FALSE
    END AS is_pending,
    f._inserted_timestamp
FROM
    flat_base f
LEFT OUTER JOIN {{ ref('silver_goerli__blocks') }} b 
    ON f.block_number = b.block_number
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp,
        t.tx_hash,
        t.nonce,
        t.POSITION,
        t.from_address,
        t.to_address,
        t.eth_value,
        block_hash,
        t.gas_price,
        t.gas_limit,
        t.input_data,
        t.tx_type,
        t.is_system_tx,
        t.tx_json,
        t.tx_status,
        t.gas_used,
        t.cumulative_gas_used,
        t.effective_gas_price,
        t.l1_fee_scalar,
        t.l1_gas_used,
        t.l1_gas_price,
        t.tx_fee,
        t.origin_function_signature,
        FALSE AS is_pending,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp
        ) AS _inserted_timestamp
    FROM {{ this }} t 
    INNER JOIN {{ ref('silver_goerli__blocks') }} b 
        ON t.block_number = b.block_number
    WHERE
        t.is_pending
)
{% endif %}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    nonce,
    POSITION,
    from_address,
    to_address,
    eth_value,
    block_hash,
    gas_price,
    gas_limit,
    input_data,
    tx_type,
    is_system_tx,
    tx_json,
    tx_status,
    gas_used,
    cumulative_gas_used,
    effective_gas_price,
    l1_fee_scalar,
    l1_gas_used,
    l1_gas_price,
    tx_fee,
    origin_function_signature,
    is_pending,
    _inserted_timestamp
FROM new_records

{% if is_incremental() %}
UNION
SELECT
    *
FROM missing_data
{% endif %}