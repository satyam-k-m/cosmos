
  
    

        create or replace transient table insight_dev.INS_BKP.bronze_pos_shop
         as
        (

---updated bronze table
with POS_SHOP as (
    select * exclude rn from (
     select
		
        
        
        
            value:c1:: NUMBER(38,4) AS POS_LOCATION_ID,
        
            value:c2:: NUMBER(38,4) AS DIVISON_NUMBER,
            
    
        HASH(concat_ws('~',pos_location_id,divison_number)) as bsns_key, 
        20230706110040 as run_id,
        row_number() over(partition by pos_location_id,divison_number order by 1) as rn

        from insight_dev.ins_bkp.ext_pos_shop
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
    where rn =1
)

select * from POS_SHOP
        );
      
  