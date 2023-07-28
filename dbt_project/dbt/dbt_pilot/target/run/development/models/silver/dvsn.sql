-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into insight_dev.INS_BKP.dvsn as DBT_INTERNAL_DEST
        using insight_dev.INS_BKP.dvsn__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.bsns_key = DBT_INTERNAL_DEST.bsns_key
            )

    
    when matched then update set
        "DVSN_NBR" = DBT_INTERNAL_SOURCE."DVSN_NBR","DVSN_NAME" = DBT_INTERNAL_SOURCE."DVSN_NAME","DVSN_ABBRV_NAME" = DBT_INTERNAL_SOURCE."DVSN_ABBRV_NAME","ADDRSS_LINE_1" = DBT_INTERNAL_SOURCE."ADDRSS_LINE_1","ADDRSS_LINE_2" = DBT_INTERNAL_SOURCE."ADDRSS_LINE_2","ADDRSS_LINE_3" = DBT_INTERNAL_SOURCE."ADDRSS_LINE_3","CITY" = DBT_INTERNAL_SOURCE."CITY","STATE" = DBT_INTERNAL_SOURCE."STATE","ZIP_CD" = DBT_INTERNAL_SOURCE."ZIP_CD","DVSN_TYPE" = DBT_INTERNAL_SOURCE."DVSN_TYPE","DVSN_SKU_TYPE" = DBT_INTERNAL_SOURCE."DVSN_SKU_TYPE","DVSN_SKU_STATUS" = DBT_INTERNAL_SOURCE."DVSN_SKU_STATUS","DFS_WRLDWD" = DBT_INTERNAL_SOURCE."DFS_WRLDWD","DVSN_RPT_HDNG" = DBT_INTERNAL_SOURCE."DVSN_RPT_HDNG","WRITE_OFF_DSCRPTN_1" = DBT_INTERNAL_SOURCE."WRITE_OFF_DSCRPTN_1","WRITE_OFF_DSCRPTN_2" = DBT_INTERNAL_SOURCE."WRITE_OFF_DSCRPTN_2","DT_FORMAT_CD" = DBT_INTERNAL_SOURCE."DT_FORMAT_CD","SALES_CNTRL_NBR" = DBT_INTERNAL_SOURCE."SALES_CNTRL_NBR","CNCL_PO_DAYS" = DBT_INTERNAL_SOURCE."CNCL_PO_DAYS","CNCL_PO_DAYS_PCT" = DBT_INTERNAL_SOURCE."CNCL_PO_DAYS_PCT","CRRNCY_CD" = DBT_INTERNAL_SOURCE."CRRNCY_CD","MULT_OTB_PRDS" = DBT_INTERNAL_SOURCE."MULT_OTB_PRDS","MULT_UNIT_RTL" = DBT_INTERNAL_SOURCE."MULT_UNIT_RTL","MULT_DPRTMNT_IN_PO" = DBT_INTERNAL_SOURCE."MULT_DPRTMNT_IN_PO","MDLS_CHCK_ROUTINE" = DBT_INTERNAL_SOURCE."MDLS_CHCK_ROUTINE","INVC_VAR_PCT" = DBT_INTERNAL_SOURCE."INVC_VAR_PCT","INVC_VAR_AMNT" = DBT_INTERNAL_SOURCE."INVC_VAR_AMNT","COST_REAVG" = DBT_INTERNAL_SOURCE."COST_REAVG","SALES_PRCSSNG_FLG" = DBT_INTERNAL_SOURCE."SALES_PRCSSNG_FLG","INVC_MNMM_CHRGBCK_AMNT" = DBT_INTERNAL_SOURCE."INVC_MNMM_CHRGBCK_AMNT","CUSTOMS_FLG" = DBT_INTERNAL_SOURCE."CUSTOMS_FLG","EXCHNG_RATE_VAR" = DBT_INTERNAL_SOURCE."EXCHNG_RATE_VAR","INVC_CUTOFF_DAY" = DBT_INTERNAL_SOURCE."INVC_CUTOFF_DAY","PLAN_LVL" = DBT_INTERNAL_SOURCE."PLAN_LVL","PRICE_LKP_FLG" = DBT_INTERNAL_SOURCE."PRICE_LKP_FLG","TRDNG_CMPNY_CD" = DBT_INTERNAL_SOURCE."TRDNG_CMPNY_CD","DB_COST_FLG" = DBT_INTERNAL_SOURCE."DB_COST_FLG","CREATE_PRORDR_TRNSFR_ADT_RCRD" = DBT_INTERNAL_SOURCE."CREATE_PRORDR_TRNSFR_ADT_RCRD","POPI_CNT_FLG" = DBT_INTERNAL_SOURCE."POPI_CNT_FLG","RCNET_CD" = DBT_INTERNAL_SOURCE."RCNET_CD","AUTMTD_WRITE_OFF_LIMIT" = DBT_INTERNAL_SOURCE."AUTMTD_WRITE_OFF_LIMIT","MRPU_ENTRY" = DBT_INTERNAL_SOURCE."MRPU_ENTRY","SCLSS_REALIGN_DRNG_PI" = DBT_INTERNAL_SOURCE."SCLSS_REALIGN_DRNG_PI","BSNS_KEY" = DBT_INTERNAL_SOURCE."BSNS_KEY","RUN_ID" = DBT_INTERNAL_SOURCE."RUN_ID","IS_SUSPENDED" = DBT_INTERNAL_SOURCE."IS_SUSPENDED","SPNS_RVRSL_TIMESTAMP" = DBT_INTERNAL_SOURCE."SPNS_RVRSL_TIMESTAMP"
    

    when not matched then insert
        ("DVSN_SRRGT_KEY", "DVSN_NBR", "DVSN_NAME", "DVSN_ABBRV_NAME", "ADDRSS_LINE_1", "ADDRSS_LINE_2", "ADDRSS_LINE_3", "CITY", "STATE", "ZIP_CD", "DVSN_TYPE", "DVSN_SKU_TYPE", "DVSN_SKU_STATUS", "DFS_WRLDWD", "DVSN_RPT_HDNG", "WRITE_OFF_DSCRPTN_1", "WRITE_OFF_DSCRPTN_2", "DT_FORMAT_CD", "SALES_CNTRL_NBR", "CNCL_PO_DAYS", "CNCL_PO_DAYS_PCT", "CRRNCY_CD", "MULT_OTB_PRDS", "MULT_UNIT_RTL", "MULT_DPRTMNT_IN_PO", "MDLS_CHCK_ROUTINE", "INVC_VAR_PCT", "INVC_VAR_AMNT", "COST_REAVG", "SALES_PRCSSNG_FLG", "INVC_MNMM_CHRGBCK_AMNT", "CUSTOMS_FLG", "EXCHNG_RATE_VAR", "INVC_CUTOFF_DAY", "PLAN_LVL", "PRICE_LKP_FLG", "TRDNG_CMPNY_CD", "DB_COST_FLG", "CREATE_PRORDR_TRNSFR_ADT_RCRD", "POPI_CNT_FLG", "RCNET_CD", "AUTMTD_WRITE_OFF_LIMIT", "MRPU_ENTRY", "SCLSS_REALIGN_DRNG_PI", "BSNS_KEY", "RUN_ID", "IS_SUSPENDED", "SPNS_RVRSL_TIMESTAMP")
    values
        ("DVSN_SRRGT_KEY", "DVSN_NBR", "DVSN_NAME", "DVSN_ABBRV_NAME", "ADDRSS_LINE_1", "ADDRSS_LINE_2", "ADDRSS_LINE_3", "CITY", "STATE", "ZIP_CD", "DVSN_TYPE", "DVSN_SKU_TYPE", "DVSN_SKU_STATUS", "DFS_WRLDWD", "DVSN_RPT_HDNG", "WRITE_OFF_DSCRPTN_1", "WRITE_OFF_DSCRPTN_2", "DT_FORMAT_CD", "SALES_CNTRL_NBR", "CNCL_PO_DAYS", "CNCL_PO_DAYS_PCT", "CRRNCY_CD", "MULT_OTB_PRDS", "MULT_UNIT_RTL", "MULT_DPRTMNT_IN_PO", "MDLS_CHCK_ROUTINE", "INVC_VAR_PCT", "INVC_VAR_AMNT", "COST_REAVG", "SALES_PRCSSNG_FLG", "INVC_MNMM_CHRGBCK_AMNT", "CUSTOMS_FLG", "EXCHNG_RATE_VAR", "INVC_CUTOFF_DAY", "PLAN_LVL", "PRICE_LKP_FLG", "TRDNG_CMPNY_CD", "DB_COST_FLG", "CREATE_PRORDR_TRNSFR_ADT_RCRD", "POPI_CNT_FLG", "RCNET_CD", "AUTMTD_WRITE_OFF_LIMIT", "MRPU_ENTRY", "SCLSS_REALIGN_DRNG_PI", "BSNS_KEY", "RUN_ID", "IS_SUSPENDED", "SPNS_RVRSL_TIMESTAMP")

;
    commit;