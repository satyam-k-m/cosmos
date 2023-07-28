
  
    

        create or replace transient table insight_dev.INS_BKP.bronze_division
         as
        (


with division as (
    select * exclude rn from (
     select
        
        
        
        
            value:c1:: NUMBER(38,4) AS DIV_NBR,
        
            value:c2:: TEXT AS DIV_NAME,
        
            value:c3:: TEXT AS ADDR_LINE_1,
        
            value:c4:: TEXT AS ADDR_LINE_2,
        
            value:c5:: TEXT AS ADDR_LINE_3,
        
            value:c6:: TEXT AS CITY,
        
            value:c7:: NUMBER(38,4) AS STATE,
        
            value:c8:: NUMBER(38,4) AS ZIP_CD,
        
            value:c9:: TEXT AS DVSN_RPT_HDNG,
        
            value:c10:: TEXT AS WRITE_OFF_DESC_1,
        
            value:c11:: TEXT AS WRIRE_OFF_DESC_2,
        
            value:c12:: TEXT AS DT_FORMAT_CD,
        
            value:c13:: NUMBER(38,4) AS SALES_CTL_NBR,
        
            value:c14:: NUMBER(38,4) AS CNCL_PO_DAYS,
        
            value:c15:: NUMBER(38,4) AS CNCL_PO_DAYS_PCT,
        
            value:c16:: TEXT AS CURR_CD,
        
            value:c17:: BOOLEAN AS MULT_OTB_PRES,
        
            value:c18:: BOOLEAN AS MULT_UNIT_RTL,
        
            value:c19:: BOOLEAN AS MULT_DEPT_IN_PO,
        
            value:c20:: NUMBER(38,4) AS MOD_CHK_ROUTINE,
        
            value:c21:: NUMBER(38,4) AS INV_VAR_PCT,
        
            value:c22:: NUMBER(38,4) AS INV_VAR_AMT,
        
            value:c23:: BOOLEAN AS COST_REAVG,
        
            value:c24:: BOOLEAN AS SALES_PROCESS_FLG,
        
            value:c25:: NUMBER(38,4) AS INV_MIN_CHGBCK_AMT,
        
            value:c26:: NUMBER(38,4) AS CUSTOMS_FLG,
        
            value:c27:: NUMBER(38,4) AS EXCH_RATE_VAR,
        
            value:c28:: NUMBER(38,4) AS INV_CUTOFF_DAY,
        
            value:c29:: NUMBER(38,4) AS PLAN_LVL,
        
            value:c30:: BOOLEAN AS PRICE_LKUP_FLG,
        
            value:c31:: NUMBER(38,4) AS TRAD_COMP_CD,
        
            value:c32:: TEXT AS DB_COST_FLG,
        
            value:c33:: BOOLEAN AS CREATE_PREORD_TRANS_AUD_REC,
        
            value:c34:: BOOLEAN AS POPI_CNT_FLG,
        
            value:c35:: NUMBER(38,4) AS RCENT_CD,
        
            value:c36:: NUMBER(38,4) AS AUTO_WRITE_OFF_LIMIT,
        
            value:c37:: BOOLEAN AS MRPU_ENTRY,
        
            value:c38:: BOOLEAN AS SCLS_REALIGN_DUR_PI,
            
    
        20230706110040 as run_id,
        HASH(DIV_NBR) as bsns_key,
        row_number() over(partition by DIV_NBR order by 1) as rn
    from insight_dev.ins_bkp.ext_dvsn
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
    where rn =1

)
select * from division
        );
      
  