
{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}

with POS_DSCNT as (
        select * exclude rn from (

     select
        {{ m_read_schema('POS_DSCNT') }}
        HASH(concat_ws('~',div_nbr,div_dscnt_cd)) as bsns_key,
        {{ var("run_id") }} as run_id,
        row_number() over(partition by div_nbr,div_dscnt_cd order by 1) as rn

        from {{source('dfs_stage','ext_pos_dscnt') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
)
where rn=1
)
select * from POS_DSCNT
