{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES, REGISTRY' } } }
) }}

SELECT
    NAME,
    description,
    registry_id,
    contract_address,
    CASE
        WHEN contract_address = '0x3c1ff68f5aa342d296d4dee4bb1cacca912d95fe' THEN 'Service'
    END AS registry_type,
    trait_type,
    trait_value,
    code_uri_link,
    image_link,
    registry_metadata_id AS dim_registry_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_olas__registry_metadata') }}
