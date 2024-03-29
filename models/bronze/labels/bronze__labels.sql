{{ config(
    materialized = 'view'
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    _is_deleted,
    modified_timestamp,
    labels_combined_id
FROM
    {{ source(
        'silver_crosschain',
        'labels_combined'
    ) }}
WHERE
    blockchain = 'base'
    AND address LIKE '0x%'
