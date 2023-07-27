
{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}

with RFND_ORGNL_TX_RFRNC as (
    select * exclude rn from (
     select
		{{ m_read_schema('RFND_TX_RF') }}
        HASH(concat_ws('~',ORIG_TX_NBR,ORIG_TERM_NBR)) as bsns_key,
        {{ var("run_id") }} as run_id,
        row_number() over(partition by ORIG_TX_NBR,ORIG_TERM_NBR order by 1) as rn
        from {{source('dfs_stage','ext_rfnd_tx_rf') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
    where rn =1
)

select * from RFND_ORGNL_TX_RFRNC
