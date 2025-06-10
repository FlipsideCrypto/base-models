{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "contract_address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}
-- busdriven + oftsent
WITH busdriven AS (

    SELECT
    FROM
        {{ ref('silver_bridge__stargate_v2_busdriven') }}
)
