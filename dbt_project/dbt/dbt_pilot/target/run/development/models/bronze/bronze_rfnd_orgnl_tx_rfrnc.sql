
  
    

        create or replace transient table insight_dev.INS_BKP.bronze_rfnd_orgnl_tx_rfrnc
         as
        (

with RFND_ORGNL_TX_RFRNC as (
    select * exclude rn from (
     select
		
        
        
            
    
        HASH(concat_ws('~',ORIG_TX_NBR,ORIG_TERM_NBR)) as bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by ORIG_TX_NBR,ORIG_TERM_NBR order by 1) as rn
        from insight_dev.ins_bkp.ext_rfnd_tx_rf
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
    where rn =1
)

select * from RFND_ORGNL_TX_RFRNC
        );
      
  