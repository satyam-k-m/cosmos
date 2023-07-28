{{
    config(
        materialized='incremental',
        unique_key='bsns_key',
        merge_exclude_columns = ['tx_type_srrgt_key'],
    )
}}

with delta as (
    select
        tx_type_seq.nextval as tx_type_srrgt_key,
        brz.tx_type,
        brz.app_cd as appln_cd,
        brz.DIV_NBR as dvsn_nbr,
        brz.SHORT_DESC as short_dscrptn,
        brz.bsns_key as bsns_key,
        brz.run_id as run_id,
        nvl(dvsn.dvsn_srrgt_key,'-1') as dvsn_srrgt_key,
        case when nvl(dvsn.dvsn_srrgt_key,'-1') = '-1' then true else false end as is_suspended ,
        null as spns_rvrsl_timestamp
    from
        {{ ref('bronze_tx_type')}} brz
    LEFT JOIN {{ref('dvsn') }} dvsn
            on brz.DIV_NBR=dvsn.dvsn_nbr

    union all

    select
        tx_type_srrgt_key,
        slv.tx_type,
        slv.appln_cd,
        slv.dvsn_nbr,
        slv.short_dscrptn,
        slv.bsns_key,
        slv.run_id,
        dvsn.dvsn_srrgt_key,
        false as is_suspended,
        current_timestamp as spns_rvrsl_timestamp
    from
        {{ this }} slv
    LEFT OUTER JOIN {{ref('dvsn') }} dvsn
        on slv.dvsn_nbr=dvsn.dvsn_nbr
    where slv.is_suspended = true
    and dvsn.dvsn_srrgt_key is not null
),

ranked_dataset as (
    select *,
    row_number() over (partition by bsns_key order by run_id desc) as rn
    from delta
)

select * exclude rn from ranked_dataset where rn = 1
