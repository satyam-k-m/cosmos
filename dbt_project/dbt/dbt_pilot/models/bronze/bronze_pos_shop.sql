
{{
    config(
        materialized='table',
        query_tag = var("system") ~ '_' ~ var("source") ~ '_' ~ var("division") ~ '_' ~ 'BRONZE'
    )
}}

---updated bronze table
with POS_SHOP as (
    select * exclude rn from (
     select
		{{ m_read_schema('POS_SHOP') }}
        HASH(concat_ws('~',pos_location_id,divison_number)) as bsns_key, 
        {{ var("run_id") }} as run_id,
        row_number() over(partition by pos_location_id,divison_number order by 1) as rn

        from {{source('dfs_stage','ext_pos_shop') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
    where rn =1
)

select * from POS_SHOP
