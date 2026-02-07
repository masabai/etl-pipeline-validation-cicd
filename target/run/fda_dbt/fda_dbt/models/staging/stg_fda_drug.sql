
  create or replace   view ETL_TESTING.STAGING_STAGING.stg_fda_drug
  
  
  
  
  as (
    select
    primaryid,
    caseid,
    drug_seq,
    role_cod,
    drugname,
    prod_ai,
    val_vbm,
    route,
    dose_vbm,
    cum_dose_chr,
    cum_dose_unit,
    dechal,
    rechal,
    lot_num,
    exp_dt,
    nda_num,
    dose_amt,
    dose_unit,
    dose_form,
    dose_freq
from ETL_TESTING.FDA.drug
  );

