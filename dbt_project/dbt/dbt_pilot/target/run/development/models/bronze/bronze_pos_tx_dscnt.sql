
  
    

        create or replace transient table insight_dev.INS_BKP.bronze_pos_tx_dscnt
         as
        (

with POS_TX_DSCNT as (
	    select * exclude rn from (

     select
		
        
        
        
            value:c1:: NUMBER(38,4) AS DSCNT_LINE_NBR,
        
            value:c2:: NUMBER(38,4) AS SALE_LINE_NBR,
        
            value:c3:: NUMBER(38,4) AS TX_NBR,
        
            value:c4:: NUMBER(38,4) AS POS_LOC_ID,
        
            value:c5:: NUMBER(38,4) AS TERM_NBR,
        
            value:c6:: NUMBER(38,4) AS DIV_NBR,
        
            value:c7:: NUMBER(38,4) AS BIZ_DT,
        
            value:c8:: TEXT AS ADJ_FLG,
        
            value:c9:: NUMBER(38,4) AS STORE_ID,
        
            value:c10:: TEXT AS DSCNT_ID_NBR,
        
            value:c11:: NUMBER(38,4) AS DIV_DSCNT_CD,
        
            value:c12:: TEXT AS DIV_DSCNT_TYPE,
        
            value:c13:: NUMBER(38,4) AS MERCH_DSCNT_CD,
        
            value:c14:: NUMBER(38,4) AS LOCAL_DSCNT_AMT,
        
            value:c15:: NUMBER(38,4) AS HOST_DSCNT_AMT,
        
            value:c16:: NUMBER(38,4) AS DSCNT_PERCENT,
        
            value:c17:: NUMBER(38,4) AS RTL_DSCNT_EMBED_TAX,
        
            value:c18:: NUMBER(38,4) AS COUP_CD,
        
            value:c19:: TEXT AS COUP_NBR,
        
            value:c20:: NUMBER(38,4) AS DSCNT_REAS_CD,
        
            value:c21:: NUMBER(38,4) AS RCPT_SEQ_NBR,
            
    
        HASH(concat_ws('~',dscnt_line_nbr,sale_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by dscnt_line_nbr,sale_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id order by 1) as rn		
		from insight_dev.ins_bkp.ext_pos_tx_dct
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
    where rn =1
)

select * from POS_TX_DSCNT
        );
      
  