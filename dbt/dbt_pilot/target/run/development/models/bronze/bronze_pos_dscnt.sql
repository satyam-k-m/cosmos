
  
    

        create or replace transient table insight_dev.INS_BKP.bronze_pos_dscnt
         as
        (

with POS_DSCNT as (
        select * exclude rn from (

     select
        
        
        
        
            value:c1:: NUMBER(38,4) AS DIV_DSCNT_CD,
        
            value:c2:: NUMBER(38,4) AS DIV_NBR,
        
            value:c3:: TEXT AS DIV_DSCNT_DESC,
        
            value:c4:: NUMBER(38,4) AS MERCH_DSCNT_CD,
        
            value:c5:: BOOLEAN AS MD_DSCNT_CD,
            
    
        HASH(concat_ws('~',div_nbr,div_dscnt_cd)) as bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by div_nbr,div_dscnt_cd order by 1) as rn

        from insight_dev.ins_bkp.ext_pos_dscnt
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
)
where rn=1
)
select * from POS_DSCNT
        );
      
  