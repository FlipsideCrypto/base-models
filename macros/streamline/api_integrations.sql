{% macro create_aws_base_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_base_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-base' api_allowed_prefixes = (
            'https://XXX.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_base_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/base-api-dev-rolesnowflakeudfsAF733095-I354FW5312ZX' api_allowed_prefixes = (
            'https://rijt3fsk7b.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
