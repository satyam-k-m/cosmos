
    
    

select
    RUN_DT as unique_field,
    count(*) as n_records

from DFS_POC_DB.P_DATA.test_pos_shop
where RUN_DT is not null
group by RUN_DT
having count(*) > 1


