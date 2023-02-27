{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['round(block_number,-3)']
) }}

WITH base AS (

    SELECT
        contract_address,
        block_hash,
        block_number,
        DATA,
        log_index,
        removed,
        topics,
        tx_hash,
        tx_index,
        _log_id,
        _inserted_timestamp
    FROM
        base.silver.goerli_logs

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
outputsproposed AS (
    SELECT
        block_number,
        tx_hash,
        log_index,
        topics,
        DATA,
        contract_address,
        'OutputProposed' AS event_name,
        object_construct_keep_null(
            'output_root',
            topics [1] :: STRING,
            'l2_output_index',
            ethereum.public.udf_hex_to_int(
                topics [2] :: STRING
            ) :: INTEGER,
            'l2_block_number',
            ethereum.public.udf_hex_to_int(
                topics [3] :: STRING
            ) :: INTEGER,
            'l1_timestamp',
            ethereum.public.udf_hex_to_int(
                DATA :: STRING
            ) :: INTEGER
        ) AS event_data,
        _inserted_timestamp,
        _log_id
    FROM
        base
    WHERE
        contract_address = '0x2a35891ff30313ccfa6ce88dcf3858bb075a2298'
        AND topics [0] :: STRING = '0xa7aaf2512769da4e444e3de247be2564225c2e7a8f74cfe528e46e17d24868e2'
),
transactionsdeposited AS (
    SELECT
        block_number,
        tx_hash,
        log_index,
        topics,
        DATA,
        contract_address,
        'TransactionDeposited' AS event_name,
        object_construct_keep_null(
            'from_address',
            CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)),
            'to_address',
            CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)),
            'version',
            ethereum.public.udf_hex_to_int(
                topics [3] :: STRING
            ),
            'opaqueData',
            DATA :: STRING
        ) AS event_data,
        _inserted_timestamp,
        _log_id
    FROM
        base
    WHERE
        contract_address = '0xe93c8cd0d409341205a592f8c4ac1a5fe5585cfa'
        AND topics [0] :: STRING = '0xb3813568d9991fc951961fcb4c784893574240a28925604d09fc577c55bb7c32'
),
withdrawalsproven AS (
    SELECT
        block_number,
        tx_hash,
        log_index,
        topics,
        DATA,
        contract_address,
        'WithdrawalProven' AS event_name,
        object_construct_keep_null(
            'withdrawal_hash',
            topics [1] :: STRING,
            'from_address',
            CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)),
            'to_address',
            CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
        ) AS event_data,
        _inserted_timestamp,
        _log_id
    FROM
        base
    WHERE
        contract_address = '0xe93c8cd0d409341205a592f8c4ac1a5fe5585cfa'
        AND topics [0] :: STRING = '0x67a6208cfcc0801d50f6cbe764733f4fddf66ac0b04442061a8a8c0cb6b63f62'
),
withdrawalsfinalized AS (
    SELECT
        block_number,
        tx_hash,
        log_index,
        topics,
        DATA,
        contract_address,
        'WithdrawalFinalized' AS event_name,
        object_construct_keep_null(
            'withdrawal_hash',
            topics [1] :: STRING,
            'success',
            ethereum.public.udf_hex_to_int(
                DATA :: STRING
            ) :: BOOLEAN
        ) AS event_data,
        _inserted_timestamp,
        _log_id
    FROM
        base
    WHERE
        contract_address = '0xe93c8cd0d409341205a592f8c4ac1a5fe5585cfa'
        AND topics [0] :: STRING = '0xdb5c7652857aa163daadd670e116628fb42e869d8ac4251ef8971d9e5727df1b'
),
FINAL AS (
    SELECT
        block_number,
        tx_hash,
        log_index,
        contract_address,
        event_name,
        event_data,
        _inserted_timestamp,
        _log_id
    FROM
        outputsproposed
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        log_index,
        contract_address,
        event_name,
        event_data,
        _inserted_timestamp,
        _log_id
    FROM
        transactionsdeposited
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        log_index,
        contract_address,
        event_name,
        event_data,
        _inserted_timestamp,
        _log_id
    FROM
        withdrawalsproven
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        log_index,
        contract_address,
        event_name,
        event_data,
        _inserted_timestamp,
        _log_id
    FROM
        withdrawalsfinalized
)
SELECT
    block_number,
    tx_hash,
    log_index,
    contract_address,
    event_name,
    event_data,
    _inserted_timestamp,
    _log_id
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY _log_id
        ORDER BY
            _inserted_timestamp
    ) = 1
