

{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}


with TX_TYPE as (
    select * exclude rn from (
     select
		{{ m_read_schema('TX_TYPE') }}
        HASH(concat_ws('~',div_nbr,app_cd,tx_type)) as bsns_key,
        {{ var("run_id") }} as run_id,
        row_number() over(partition by div_nbr,app_cd,tx_type order by 1) as rn
        from {{source('dfs_stage','ext_tx_type') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
    where rn =1
)

select * from TX_TYPE
