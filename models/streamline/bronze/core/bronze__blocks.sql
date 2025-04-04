{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_core']
) }}

{# Main query starts here #}
{{ fsc_evm.streamline_external_table_query(
    source_name = 'blocks',
    source_version = 'v2'
) }}