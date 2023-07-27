
  
    

        create or replace transient table insight_dev.INS_BKP.bronze_pos_tndr
         as
        (

with POS_TNDR as (
	    select * exclude rn from (

     select
		
        
        
        
            value:c1:: NUMBER(38,4) AS PAY_LINE_NBR,
        
            value:c2:: NUMBER(38,4) AS TX_NBR,
        
            value:c3:: NUMBER(38,4) AS DIV_NBR,
        
            value:c4:: NUMBER(38,4) AS TERM_NBR,
        
            value:c5:: NUMBER(38,4) AS POS_LOC_ID,
        
            value:c6:: NUMBER(38,4) AS BIZ_DT,
        
            value:c7:: TEXT AS ADJ_FLG,
        
            value:c8:: NUMBER(38,4) AS CURR_CD,
        
            value:c9:: NUMBER(38,4) AS PAY_MTHD,
        
            value:c10:: NUMBER(38,4) AS FOR_CURR,
        
            value:c11:: NUMBER(38,4) AS TEN_AMT,
        
            value:c12:: NUMBER(38,4) AS EXCH_RATE,
        
            value:c13:: NUMBER(38,4) AS RCPT_SEQ_NBR,
        
            value:c14:: NUMBER(38,4) AS ADDITION_DATA,
        
            value:c15:: NUMBER(38,4) AS CHNG_RND,
            
    
        HASH(concat_ws('~',pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id order by 1) as rn
		from insight_dev.ins_bkp.ext_pos_tndr
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
where rn=1
)

select * from POS_TNDR
        );
      
  