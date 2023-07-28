select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from DFS_POC_DB.P_DATA_dbt_test__audit.older_date
    
      
    ) dbt_internal_test