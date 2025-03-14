{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_core_streamline_v1','bronze_receipts']
) }}

{# Main query starts here #}
{{ fsc_evm.streamline_external_table_query_fr(
    source_name = 'receipts',
    source_version = '',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_join_key = "_partition_by_block_id",
    block_number = false
) }}
