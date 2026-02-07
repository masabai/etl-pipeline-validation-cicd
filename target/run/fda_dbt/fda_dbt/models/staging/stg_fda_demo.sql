
  create or replace   view ETL_TESTING.STAGING_STAGING.stg_fda_demo
  
  
  
  
  as (
    select
    primaryid,
    caseid,
    caseversion,
    i_f_code,
    event_dt,
    mfr_dt,
    init_fda_dt,
    fda_dt,
    rept_cod,
    auth_num,
    mfr_num,
    mfr_sndr,
    lit_ref,
    age,
    age_cod,
    age_grp,
    sex,
    e_sub,
    wt,
    wt_cod,
    rept_dt,
    to_mfr,
    occp_cod,
    reporter_country,
    occr_country
from ETL_TESTING.FDA.demo
  );

