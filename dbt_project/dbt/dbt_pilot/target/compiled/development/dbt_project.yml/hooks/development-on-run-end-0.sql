

  create or replace table P_DATA.test_failure_central as (
  
  
  
    select
      'older_date' as test_name,
      object_construct_keep_null(*) as test_failures_json
      
    from DFS_POC_DB.P_DATA_dbt_test__audit.older_date
    
    union all
  
  
  
    select
      'unique_test_pos_shop_RUN_DT' as test_name,
      object_construct_keep_null(*) as test_failures_json
      
    from DFS_POC_DB.P_DATA_dbt_test__audit.unique_test_pos_shop_RUN_DT
    
    
  
  
  
  )

