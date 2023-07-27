
  
    

        create or replace transient table DFS_POC_DB.P_DATA.test_pos_shop
         as
        (select * from  dfs_poc_db.p_data.ext_pos_shop
        );
      
  