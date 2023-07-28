
{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}

WITH LOCAL_CURRENCY AS (
        select * exclude rn from (

    SELECT 
        {{ m_read_schema('LCL_CRRNCY') }}
        HASH(CONCAT_WS('~',CURR_CD,DIV_NBR)) AS bsns_key,
        {{ var("run_id") }} as run_id,
        row_number() over(partition by CURR_CD,DIV_NBR order by 1) as rn
    FROM {{ source('dfs_stage', 'ext_lcl_crrncy') }}
    where
    division =  '{{ var("division") }}'
    and run_dt = substr('{{ var("run_id") }}',0,8)

        )
        where rn  = 1
)
SELECT *
FROM LOCAL_CURRENCY
