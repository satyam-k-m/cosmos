{{
    config(
        materialized='incremental',
        unique_key='bsns_key',
        merge_exclude_columns = ['pos_trmnl_srrgt_key'],
    )
}}

with delta as (
    select
        pos_trmnl_seq.nextval as pos_trmnl_srrgt_key,
        ps.POS_SHOP_SRRGT_KEY as pos_shop_srrgt_key,
        brz.POS_LOCATION_ID as pos_lctn_id,
        brz.bsns_key as bsns_key,
        brz.DIVISON_NUMBER as dvsn_nbr,
        brz.TERMINAL_NUMBER as trmnl_nbr,
        brz.run_id as run_id,
        case when nvl(ps.POS_SHOP_SRRGT_KEY,'-1') = '-1' then true else false end as is_suspended,
        null as spns_rvrsl_timestamp
    from {{ ref('bronze_pos_trmnl')}} brz
    left outer join {{ ref('pos_shop')}} ps
        on brz.POS_LOCATION_ID = ps.poc_lctn_id and brz.DIVISON_NUMBER = ps.dvsn_nbr

    union all

    select
        slv.pos_trmnl_srrgt_key,
        ps.POS_SHOP_SRRGT_KEY as pos_shop_srrgt_key,
        slv.pos_lctn_id,
        slv.bsns_key,
        slv.dvsn_nbr,
        slv.trmnl_nbr,
        slv.run_id as run_id,
        false as is_suspended,
        current_timestamp as spns_rvrsl_timestamp
    from {{ this }} slv
    left outer join {{ ref('pos_shop')}} ps on slv.pos_lctn_id = ps.poc_lctn_id and slv.dvsn_nbr = ps.dvsn_nbr
    where slv.is_suspended = true
        and ps.pos_shop_srrgt_key is not null
),


ranked_dataset as (
    select *,
        row_number() over (partition by bsns_key order by run_id desc) as rn
    from delta
)

select * exclude rn from ranked_dataset where rn = 1

