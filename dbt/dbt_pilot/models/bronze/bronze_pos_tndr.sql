

{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}

with POS_TNDR as (
	    select * exclude rn from (

     select
		{{ m_read_schema('POS_TNDR') }}
        HASH(concat_ws('~',pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as bsns_key,
        {{ var("run_id") }} as run_id,
        row_number() over(partition by pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id order by 1) as rn
		from {{source('dfs_stage','ext_pos_tndr') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
where rn=1
)

select * from POS_TNDR
