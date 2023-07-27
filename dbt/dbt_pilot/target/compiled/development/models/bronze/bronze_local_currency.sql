

WITH LOCAL_CURRENCY AS (
        select * exclude rn from (

    SELECT 
        
        
        
        
            value:c1:: NUMBER(38,4) AS CURR_CD,
        
            value:c2:: NUMBER(38,4) AS DIV_NBR,
        
            value:c3:: TEXT AS CURR_DESC,
        
            value:c4:: TEXT AS CURR_SHORT_DESC,
        
            value:c5:: BOOLEAN AS LOCAL_CURR_IND,
        
            value:c6:: NUMBER(38,4) AS CURR_UNIT,
            
    
        HASH(CONCAT_WS('~',CURR_CD,DIV_NBR)) AS bsns_key,
        20230706110040 as run_id,
        row_number() over(partition by CURR_CD,DIV_NBR order by 1) as rn
    FROM insight_dev.ins_bkp.ext_lcl_crrncy
    where
    division =  'SG'
    and run_dt = substr('20230706110040',0,8)

        )
        where rn  = 1
)
SELECT *
FROM LOCAL_CURRENCY