-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into insight_dev.INS_BKP.pos_trmnl as DBT_INTERNAL_DEST
        using insight_dev.INS_BKP.pos_trmnl__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.bsns_key = DBT_INTERNAL_DEST.bsns_key
            )

    
    when matched then update set
        "POS_SHOP_SRRGT_KEY" = DBT_INTERNAL_SOURCE."POS_SHOP_SRRGT_KEY","POS_LCTN_ID" = DBT_INTERNAL_SOURCE."POS_LCTN_ID","BSNS_KEY" = DBT_INTERNAL_SOURCE."BSNS_KEY","DVSN_NBR" = DBT_INTERNAL_SOURCE."DVSN_NBR","TRMNL_NBR" = DBT_INTERNAL_SOURCE."TRMNL_NBR","RUN_ID" = DBT_INTERNAL_SOURCE."RUN_ID","IS_SUSPENDED" = DBT_INTERNAL_SOURCE."IS_SUSPENDED","SPNS_RVRSL_TIMESTAMP" = DBT_INTERNAL_SOURCE."SPNS_RVRSL_TIMESTAMP"
    

    when not matched then insert
        ("POS_TRMNL_SRRGT_KEY", "POS_SHOP_SRRGT_KEY", "POS_LCTN_ID", "BSNS_KEY", "DVSN_NBR", "TRMNL_NBR", "RUN_ID", "IS_SUSPENDED", "SPNS_RVRSL_TIMESTAMP")
    values
        ("POS_TRMNL_SRRGT_KEY", "POS_SHOP_SRRGT_KEY", "POS_LCTN_ID", "BSNS_KEY", "DVSN_NBR", "TRMNL_NBR", "RUN_ID", "IS_SUSPENDED", "SPNS_RVRSL_TIMESTAMP")

;
    commit;