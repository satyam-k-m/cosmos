


with POS_TX_LINE as (
    select * exclude rn from (
     select
		
        
        
        
            value:c1:: NUMBER(38,4) AS SALE_LINE_NBR,
        
            value:c2:: NUMBER(38,4) AS PROCESS_DT,
        
            value:c3:: NUMBER(38,4) AS DIVISON_NUMBER,
        
            value:c4:: NUMBER(38,4) AS TX_NBR,
        
            value:c5:: NUMBER(38,4) AS DIV_NBR,
        
            value:c6:: NUMBER(38,4) AS POS_LOC_ID,
        
            value:c7:: NUMBER(38,4) AS TERM_NBR,
        
            value:c8:: NUMBER(38,4) AS BIZ_DT,
        
            value:c9:: TEXT AS ADJ_FLG,
        
            value:c10:: NUMBER(38,4) AS STORE_ID,
        
            value:c11:: NUMBER(38,4) AS PROMO_CD,
        
            value:c12:: NUMBER(38,4) AS DSKU_ID,
        
            value:c13:: NUMBER(38,4) AS CSKU_ID,
        
            value:c14:: NUMBER(38,4) AS LINE_GRP_NBR,
        
            value:c15:: BOOLEAN AS PREORD_FLG,
        
            value:c16:: NUMBER(38,4) AS INV_RLS_LOC,
        
            value:c17:: NUMBER(38,4) AS UNIT_PRICE,
        
            value:c18:: NUMBER(38,4) AS ITEM_QTY,
        
            value:c19:: NUMBER(38,4) AS EXT_RTL,
        
            value:c20:: NUMBER(38,4) AS PRICE_OVERRIDE,
        
            value:c21:: NUMBER(38,4) AS ORIG_PRICE,
        
            value:c22:: BOOLEAN AS POST_TO_MCS,
        
            value:c23:: TEXT AS DUTY_TYPE,
        
            value:c24:: BOOLEAN AS TAX_FLG_1,
        
            value:c25:: TEXT AS TAX_FLG_2,
        
            value:c26:: BOOLEAN AS TAX_FLG_3,
        
            value:c27:: BOOLEAN AS TAX_FLG_4,
        
            value:c28:: NUMBER(38,4) AS LOCAL_TAX_1,
        
            value:c29:: NUMBER(38,4) AS LOCAL_TAX_2,
        
            value:c30:: NUMBER(38,4) AS LOCAL_TAX_3,
        
            value:c31:: NUMBER(38,4) AS LOCAL_TAX_4,
        
            value:c32:: NUMBER(38,4) AS RTL_EMBED_TAX,
        
            value:c33:: NUMBER(38,4) AS RTL_EMBED_TAX_RATE,
        
            value:c34:: NUMBER(38,4) AS LOCAL_NET_AMT,
        
            value:c35:: NUMBER(38,4) AS HOST_NET_AMT,
        
            value:c36:: NUMBER(38,4) AS HOST_TAX_1,
        
            value:c37:: NUMBER(38,4) AS HOST_TAX_2,
        
            value:c38:: NUMBER(38,4) AS HOST_TAX_3,
        
            value:c39:: NUMBER(38,4) AS HOST_TAX_4,
        
            value:c40:: NUMBER(38,4) AS SA_ID,
        
            value:c41:: NUMBER(38,4) AS COUP_CD,
        
            value:c42:: TEXT AS COUP_NBR,
        
            value:c43:: NUMBER(38,4) AS ORIG_SA_ID,
        
            value:c44:: NUMBER(38,4) AS RCPT_SEQ_NBR,
        
            value:c45:: NUMBER(38,4) AS ENT_DATA,
        
            value:c46:: TEXT AS DIV_DATA_1,
        
            value:c47:: TEXT AS DIV_DATA_2,
        
            value:c48:: NUMBER(38,4) AS PLU_LOC_NBR,
        
            value:c49:: TEXT AS NOT_AUTH_FLG,
        
            value:c50:: TEXT AS ITEM_PICK_TYPE,
        
            value:c51:: NUMBER(38,4) AS HOST_CURR_UNIT_PRICE,
        
            value:c52:: NUMBER(38,4) AS HOST_CURR_EXT_PRICE,
        
            value:c53:: NUMBER(38,4) AS DUTY_PAY_AMT,
        
            value:c54:: TEXT AS IGATE_REFERENCE,
            
    
        HASH(concat_ws('~',sale_line_nbr,tx_nbr,term_nbr,divison_number,pos_loc_id,div_nbr)) as bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by sale_line_nbr,tx_nbr,term_nbr,divison_number,pos_loc_id,div_nbr order by 1) as rn

        from insight_dev.ins_bkp.ext_pos_tx_ln
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
    where rn =1
)

select * from POS_TX_LINE