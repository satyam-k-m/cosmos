

WITH POS_TERMINAL AS (
    SELECT 
        VALUE:c1::NUMBER(38, 0) AS POS_LOCATION_ID,
        VALUE:c2::NUMBER(38, 0) AS DIVISON_NUMBER,
        VALUE:c3::NUMBER(38, 0) AS TERMINAL_NUMBER,
        DIVISION,
        RUN_DT,
        SHA2_HEX(concat_ws('~',POS_LOCATION_ID,DIVISON_NUMBER,TERMINAL_NUMBER)) AS SURR_KEY,
        TO_TIMESTAMP('2023-06-22') AS RUN_TS
    FROM insight_dev.ins_bkp.ext_pos_trmnl
)
SELECT *
FROM POS_TERMINAL