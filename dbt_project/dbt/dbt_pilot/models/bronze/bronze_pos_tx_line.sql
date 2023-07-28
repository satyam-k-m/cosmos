

{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}


with POS_TX_LINE as (
    select * exclude rn from (
     select
		{{ m_read_schema('POS_TX_LN') }}
        HASH(concat_ws('~',sale_line_nbr,tx_nbr,term_nbr,divison_number,pos_loc_id,div_nbr)) as bsns_key,
        {{ var("run_id") }} as run_id,
        row_number() over(partition by sale_line_nbr,tx_nbr,term_nbr,divison_number,pos_loc_id,div_nbr order by 1) as rn

        from {{source('dfs_stage','ext_pos_tx_ln') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
    where rn =1
)

select * from POS_TX_LINE
