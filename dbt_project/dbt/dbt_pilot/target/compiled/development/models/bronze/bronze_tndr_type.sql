


with TNDR_TYPE as (
    select * exclude rn from (
     select
		
        
        
        
            value:c1:: NUMBER(38,4) AS DIV_NBR,
        
            value:c2:: NUMBER(38,4) AS TEN_TYPE,
        
            value:c3:: TEXT AS SHORT_DESC,
        
            value:c4:: TEXT AS LONG_DESC,
        
            value:c5:: TEXT AS CURR_CARD_TYPE,
        
            value:c6:: NUMBER(38,4) AS SRC_UPDT_DT,
            
    
        HASH(concat_ws('~',ten_type,div_nbr)) as bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by ten_type,div_nbr order by 1) as rn
        from insight_dev.ins_bkp.ext_tndr_type
        where
        division =  'SG'
        and run_dt = substr('20230706110040',0,8)
    )
    where rn =1

)

select * from TNDR_TYPE