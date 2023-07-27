--checking if commits
{{
    config(
        materialized='incremental',
        unique_key='bsns_key',
        merge_exclude_columns = ['DVSN_SRRGT_KEY'],

    )
}}

with delta as (
    select
            dvsn_seq.nextval as DVSN_SRRGT_KEY,
            brz.DIV_NBR as DVSN_NBR,
            brz.DIV_NAME  as DVSN_NAME,
            null as DVSN_ABBRV_NAME,
            brz.ADDR_LINE_1  as ADDRSS_LINE_1,
            brz.ADDR_LINE_2  as ADDRSS_LINE_2,
            brz.ADDR_LINE_3 as ADDRSS_LINE_3,
            brz.CITY as CITY,
            brz.STATE as STATE,
            brz.ZIP_CD as ZIP_CD,
            null as DVSN_TYPE,
            null as DVSN_SKU_TYPE,
            null as DVSN_SKU_STATUS,
            null as DFS_WRLDWD,
            brz.DVSN_RPT_HDNG as DVSN_RPT_HDNG,
            brz.WRITE_OFF_DESC_1 as WRITE_OFF_DSCRPTN_1,
            brz.WRIRE_OFF_DESC_2 as WRITE_OFF_DSCRPTN_2,
            brz.DT_FORMAT_CD as DT_FORMAT_CD,
            brz.SALES_CTL_NBR as SALES_CNTRL_NBR,
            brz.CNCL_PO_DAYS as CNCL_PO_DAYS,
            brz.CNCL_PO_DAYS_PCT as CNCL_PO_DAYS_PCT,
            brz.CURR_CD as CRRNCY_CD,
            brz.MULT_OTB_PRES as MULT_OTB_PRDS,
            brz.MULT_UNIT_RTL as MULT_UNIT_RTL,
            brz.MULT_DEPT_IN_PO as MULT_DPRTMNT_IN_PO,
            brz.MOD_CHK_ROUTINE as MDLS_CHCK_ROUTINE,
            brz.INV_VAR_PCT as INVC_VAR_PCT,
            brz.INV_VAR_AMT as INVC_VAR_AMNT,
            brz.COST_REAVG as COST_REAVG,
            brz.SALES_PROCESS_FLG as SALES_PRCSSNG_FLG,
            brz.INV_MIN_CHGBCK_AMT as INVC_MNMM_CHRGBCK_AMNT,
            brz.CUSTOMS_FLG as CUSTOMS_FLG,
            brz.EXCH_RATE_VAR as EXCHNG_RATE_VAR,
            brz.INV_CUTOFF_DAY as INVC_CUTOFF_DAY,
            brz.PLAN_LVL as PLAN_LVL,
            brz.PRICE_LKUP_FLG as PRICE_LKP_FLG,
            brz.TRAD_COMP_CD as TRDNG_CMPNY_CD,
            brz.DB_COST_FLG as DB_COST_FLG,
            brz.CREATE_PREORD_TRANS_AUD_REC as CREATE_PRORDR_TRNSFR_ADT_RCRD,
            brz.POPI_CNT_FLG as POPI_CNT_FLG,
            brz.RCENT_CD as RCNET_CD,
            brz.AUTO_WRITE_OFF_LIMIT as AUTMTD_WRITE_OFF_LIMIT,
            brz.MRPU_ENTRY as MRPU_ENTRY,
            brz.SCLS_REALIGN_DUR_PI as SCLSS_REALIGN_DRNG_PI,
            brz.bsns_key as bsns_key,
            brz.run_id as run_id,
            false as is_suspended ,
            null as spns_rvrsl_timestamp
    from
            {{ ref('bronze_division')}} brz
            

)

select *  from delta
