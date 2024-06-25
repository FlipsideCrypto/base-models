{{ config(
    materialized = 'view'
) }}

SELECT
    m.name,
    m.description,
    m.registry_id,
    m.contract_address,
    CASE
        WHEN m.contract_address = '0x3c1ff68f5aa342d296d4dee4bb1cacca912d95fe' THEN 'Service'
    END AS registry_type,
    m.trait_type,
    m.trait_value,
    m.code_uri_link,
    m.image_link,
    s.agent_ids,
    m.registry_metadata_id,
    m.inserted_timestamp,
    GREATEST(
        COALESCE(
            m.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            s.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver_olas__registry_metadata') }}
    m
    LEFT JOIN {{ ref('silver_olas__getservice_reads') }}
    s
    ON m.registry_id = s.function_input