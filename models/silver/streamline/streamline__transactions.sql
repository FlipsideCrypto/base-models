{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    tx_hash :: STRING as tx_hash,
    block_number
FROM
    {{ ref('silver_goerli__transactions') }}
