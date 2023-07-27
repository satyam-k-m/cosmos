

with pos_tx as (
        select * exclude rn from (
    select
		
        
        
        
            value:c1:: NUMBER(38,4) AS TX_NBR,
        
            value:c2:: NUMBER(38,4) AS POS_LOC_ID,
        
            value:c3:: NUMBER(38,4) AS DIV_NBR,
        
            value:c4:: NUMBER(38,4) AS TERM_NBR,
        
            value:c5:: NUMBER(38,4) AS BIZ_DT,
        
            value:c6:: TEXT AS ADJ_FLG,
        
            value:c7:: NUMBER(38,4) AS TX_DT,
        
            value:c8:: NUMBER(38,4) AS TX_TIME,
        
            value:c9:: TEXT AS CURR_CD,
        
            value:c10:: NUMBER(38,4) AS POS_TX_TYPE,
        
            value:c11:: NUMBER(38,4) AS MERCH_TX_TYPE,
        
            value:c12:: NUMBER(38,4) AS CUST_ID,
        
            value:c13:: NUMBER(38,4) AS LOCAL_NAT_CD,
        
            value:c14:: TEXT AS REKEY_IND,
        
            value:c15:: NUMBER(38,4) AS ITEM_CNT,
        
            value:c16:: NUMBER(38,4) AS CASHIER_NBR,
        
            value:c17:: NUMBER(38,4) AS LOCAL_NET_AMT,
        
            value:c18:: NUMBER(38,4) AS LOCAL_SUBTOT,
        
            value:c19:: NUMBER(38,4) AS LOCAL_TOT_TAX,
        
            value:c20:: NUMBER(38,4) AS TOT_PAY_AMT,
        
            value:c21:: TEXT AS HOST_CUR_CD,
        
            value:c22:: NUMBER(38,4) AS HOST_NET_AMT,
        
            value:c23:: NUMBER(38,4) AS HOST_SUBTOT,
        
            value:c24:: NUMBER(38,4) AS HOST_TOT_TAX,
        
            value:c25:: NUMBER(38,4) AS RFND_REAS,
        
            value:c26:: TEXT AS DUTY_PAY_FLAG,
        
            value:c27:: NUMBER(38,4) AS MEMBERSHIP_CARD_NUMBER,
        
            value:c28:: TEXT AS CUST_FNAME,
        
            value:c29:: TEXT AS CUST_LNAME,
        
            value:c30:: TEXT AS CUST_GNDR,
        
            value:c31:: TEXT AS CUST_PSPRT,
        
            value:c32:: TEXT AS CUST_FLT,
        
            value:c33:: NUMBER(38,4) AS CUST_DEPDATE,
        
            value:c34:: TEXT AS FLT_DST,
        
            value:c35:: TEXT AS FLT_CLSS,
        
            value:c36:: TEXT AS FLT_DPNT,
        
            value:c37:: TEXT AS FLT_SEAT,
        
            value:c38:: TEXT AS FLT_CFLT,
        
            value:c39:: NUMBER(38,4) AS FLT_CDEPDATE,
        
            value:c40:: TEXT AS FLT_CDST,
        
            value:c41:: NUMBER(38,4) AS ID_TYPE,
        
            value:c42:: NUMBER(38,4) AS ID_NO,
            
    
		HASH(concat_ws('~',TX_NBR,POS_LOC_ID,DIV_NBR,TERM_NBR)) as bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by TX_NBR,POS_LOC_ID,DIV_NBR,TERM_NBR order by 1) as rn
        from insight_dev.ins_bkp.ext_pos_tx
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
    where rn =1
)
		
select * from pos_tx