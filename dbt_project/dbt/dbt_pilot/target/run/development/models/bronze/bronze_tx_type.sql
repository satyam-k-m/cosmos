
  
    

        create or replace transient table insight_dev.INS_BKP.bronze_tx_type
         as
        (


with TX_TYPE as (
    select * exclude rn from (
     select
		
        
        
        
            value:c1:: NUMBER(38,4) AS DIV_NBR,
        
            value:c2:: NUMBER(38,4) AS APP_CD,
        
            value:c3:: NUMBER(38,4) AS TX_TYPE,
        
            value:c4:: TEXT AS SHORT_DESC,
            
    
        HASH(concat_ws('~',div_nbr,app_cd,tx_type)) as bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by div_nbr,app_cd,tx_type order by 1) as rn
        from insight_dev.ins_bkp.ext_tx_type
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
    where rn =1
)

select * from TX_TYPE
        );
      
  