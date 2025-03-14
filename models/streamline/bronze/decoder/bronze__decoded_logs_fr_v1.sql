{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_decoded_logs_streamline_v1']
) }}

{# Main query starts here #}
{{ fsc_evm.streamline_external_table_query_decoder(
    source_name = 'decoded_logs'
) }}