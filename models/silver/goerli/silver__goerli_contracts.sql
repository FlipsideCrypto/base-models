{{ config (
    materialized = "view"
) }}

WITH base AS (

    SELECT
        '0x8D85C4b3d49e29a208Ecd53362D8da3230EefcC4' AS contract_address,
        'Output Proposer' AS contract_name
    UNION ALL
    SELECT
        '0x2A35891ff30313CcFa6CE88dcf3858bb075A2298' AS contract_address,
        'Output Proposal' AS contract_name
    UNION ALL
    SELECT
        '0xe93c8cd0d409341205a592f8c4ac1a5fe5585cfa' AS contract_address,
        'Portal Proxy (Bridge)' AS contract_name
)
SELECT
    LOWER(contract_address) AS contract_address,
    contract_name
FROM
    base
