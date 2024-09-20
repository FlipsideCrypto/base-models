{{ config (
    materialized = 'incremental',
    unique_key = ["parent_contract_address","function_signature","start_block"],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['abis']
) }}
{{ fsc_evm.silver_complete_function_abis () }}
