{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;
        {{ create_udtf_get_base_table(
            schema = "streamline"
        ) }}

        {% endset %}
        {% do run_query(sql) %}
        {% if target.database != "BASE_COMMUNITY_DEV" %}
            {% set sql %}
            {{ create_udf_get_chainhead() }}
            {{ create_udf_bulk_json_rpc() }}
            {{ create_udf_bulk_json_rpc_block_id() }}

            {% endset %}
            {% do run_query(sql) %}
        {% endif %}
    {% endif %}
{% endmacro %}