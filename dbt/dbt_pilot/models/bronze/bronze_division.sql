
{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'

        )
}}


with division as (
    select * exclude rn from (
     select
        {{ m_read_schema('DVSN') }}
        {{ var("run_id") }} as run_id,
        HASH(DIV_NBR) as bsns_key,
        row_number() over(partition by DIV_NBR order by 1) as rn
    from {{source('dfs_stage','ext_dvsn') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
    where rn =1

)
select * from division
