{{ config(materialized='table') }}

with src as (

    select *
    from {{ ref('stg_fda_demo') }}

),

cleaned as (

    select
        primaryid,
        caseid,
        caseversion::int as caseversion,
        i_f_code,
        try_to_date(event_dt, 'YYYY-MM-DD') as event_dt,
        try_to_date(mfr_dt, 'YYYY-MM-DD') as mfr_dt,
        try_to_date(init_fda_dt, 'YYYY-MM-DD') as init_fda_dt,
        try_to_date(fda_dt, 'YYYY-MM-DD') as fda_dt,
        rept_cod,
        auth_num,
        mfr_num,
        mfr_sndr,
        lit_ref,
        try_to_number(age) as age,
        age_cod,
        age_grp,
        case when upper(sex) in ('M','F') then upper(sex) else 'UNK' end as sex,
        e_sub,
        try_to_number(wt) as wt,
        wt_cod,
        try_to_date(rept_dt, 'YYYY-MM-DD') as rept_dt,
        to_mfr,
        occp_cod,
        reporter_country,
        occr_country
    from src

),

final as (

    select
        row_number() over(order by primaryid, caseversion) as case_sk,
        *
    from cleaned

)

select *
from final
