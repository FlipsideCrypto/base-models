{% macro create_udf_get_chainhead() %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead(
    ) returns variant api_integration = aws_base_api AS {% if target.name == "prod" %}
        'https://avaxk4phkl.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://k9b03inxm4.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
        json variant
    ) returns text api_integration = aws_base_api AS {% if target.name == "prod" %}
        'https://avaxk4phkl.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_json_rpc'
    {% else %}
        'https://k9b03inxm4.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc_block_id() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc_block_id(
        json variant
    ) returns text api_integration = aws_base_api AS {% if target.name == "prod" %}
        'https://avaxk4phkl.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_json_rpc_block_id'
    {% else %}
        'https://k9b03inxm4.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc_block_id'
    {%- endif %};
{% endmacro %}
