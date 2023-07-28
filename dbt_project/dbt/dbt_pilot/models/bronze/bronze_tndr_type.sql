
{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}


with TNDR_TYPE as (
    select * exclude rn from (
     select
		{{ m_read_schema('TNDR_TYPE') }}
        HASH(concat_ws('~',ten_type,div_nbr)) as bsns_key,
        {{ var("run_id") }} as run_id,
        row_number() over(partition by ten_type,div_nbr order by 1) as rn
        from {{source('dfs_stage','ext_tndr_type') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
    where rn =1

)

select * from TNDR_TYPE
