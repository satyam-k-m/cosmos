-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into insight_dev.INS_BKP.tx_type as DBT_INTERNAL_DEST
        using insight_dev.INS_BKP.tx_type__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.bsns_key = DBT_INTERNAL_DEST.bsns_key
            )

    
    when matched then update set
        "TX_TYPE" = DBT_INTERNAL_SOURCE."TX_TYPE","APPLN_CD" = DBT_INTERNAL_SOURCE."APPLN_CD","DVSN_NBR" = DBT_INTERNAL_SOURCE."DVSN_NBR","SHORT_DSCRPTN" = DBT_INTERNAL_SOURCE."SHORT_DSCRPTN","BSNS_KEY" = DBT_INTERNAL_SOURCE."BSNS_KEY","RUN_ID" = DBT_INTERNAL_SOURCE."RUN_ID","DVSN_SRRGT_KEY" = DBT_INTERNAL_SOURCE."DVSN_SRRGT_KEY","IS_SUSPENDED" = DBT_INTERNAL_SOURCE."IS_SUSPENDED","SPNS_RVRSL_TIMESTAMP" = DBT_INTERNAL_SOURCE."SPNS_RVRSL_TIMESTAMP"
    

    when not matched then insert
        ("TX_TYPE_SRRGT_KEY", "TX_TYPE", "APPLN_CD", "DVSN_NBR", "SHORT_DSCRPTN", "BSNS_KEY", "RUN_ID", "DVSN_SRRGT_KEY", "IS_SUSPENDED", "SPNS_RVRSL_TIMESTAMP")
    values
        ("TX_TYPE_SRRGT_KEY", "TX_TYPE", "APPLN_CD", "DVSN_NBR", "SHORT_DSCRPTN", "BSNS_KEY", "RUN_ID", "DVSN_SRRGT_KEY", "IS_SUSPENDED", "SPNS_RVRSL_TIMESTAMP")

;
    commit;