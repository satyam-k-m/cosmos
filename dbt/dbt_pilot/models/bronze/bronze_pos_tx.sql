
{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}

with pos_tx as (
        select * exclude rn from (
    select
		{{ m_read_schema('POS_TX') }}
		HASH(concat_ws('~',TX_NBR,POS_LOC_ID,DIV_NBR,TERM_NBR)) as bsns_key,
        {{ var("run_id") }} as run_id,
        row_number() over(partition by TX_NBR,POS_LOC_ID,DIV_NBR,TERM_NBR order by 1) as rn
        from {{source('dfs_stage','ext_pos_tx') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
    where rn =1
)
		
select * from pos_tx
