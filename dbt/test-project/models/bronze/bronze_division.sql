with division as (
    select * 
    from {{source('dfs_stage','ext_dvsn') }}

)
select * from division
