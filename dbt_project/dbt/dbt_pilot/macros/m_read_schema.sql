{% macro m_read_schema(table, database = target.database, schema= target.schema) -%}
    {% if execute %}
        {% set get_schema_query %}
            select
            'value:c' || to_varchar(column_index+1) || ':: ' || type || ' AS ' || column_Name
            from {{ database }}.{{ schema }}.config_schema
            where database_name= upper('{{ database }}')
            and schema_name = upper('{{ schema }}')
            and table_name= upper('{{ table }}')
            order by column_index
        {% endset %}
        {% set schema_queries = run_query(get_schema_query).columns[0].values() %}
        {% for query in schema_queries %}
            {{ query }},
        {% endfor %}    
    {% else %}
        1 
    {% endif %}
{%- endmacro %}