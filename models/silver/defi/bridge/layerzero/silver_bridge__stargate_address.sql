WITH stargate_address AS (
    SELECT
        *
    FROM
        (
            VALUES
                (
                    LOWER('0x5634c4a5FEd09819E3c46D86A965Dd9447d86e47'),
                    'base',
                    30184,
                    'token_messaging'
                ) -- another one for base but unsused 0x160345fC359604fC6e70E3c5fAcbdE5F7A9342d8
        ) t (
            address,
            chain,
            chain_id,
            TYPE
        )
)
SELECT
    *
FROM
    stargate_address
