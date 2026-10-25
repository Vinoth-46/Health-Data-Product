{% macro mask_email(column_name) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN POSITION('@' IN {{ column_name }}) > 2 THEN
            CONCAT(
                SUBSTRING({{ column_name }} FROM 1 FOR 1),
                '***',
                SUBSTRING({{ column_name }} FROM POSITION('@' IN {{ column_name }}))
            )
        ELSE '***@redhat-anonymized.com'
    END
{% endmacro %}

{% macro mask_name(column_name) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN LENGTH(TRIM({{ column_name }})) > 2 THEN
            CONCAT(
                SUBSTRING(TRIM({{ column_name }}) FROM 1 FOR 1),
                '***',
                SUBSTRING(TRIM({{ column_name }}) FROM LENGTH(TRIM({{ column_name }})) FOR 1)
            )
        ELSE 'Anonymous Contributor'
    END
{% endmacro %}

{% macro hash_pseudonym(column_name) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN NULL
        ELSE MD5(LOWER(TRIM({{ column_name }})))
    END
{% endmacro %}
