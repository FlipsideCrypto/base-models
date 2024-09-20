{{ config (
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'contract_address',
    cluster_by = '_inserted_timestamp::date',
    tags = ['abis'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY (contract_address)"
) }}
{{ fsc_evm.silver_flat_function_abis () }}
