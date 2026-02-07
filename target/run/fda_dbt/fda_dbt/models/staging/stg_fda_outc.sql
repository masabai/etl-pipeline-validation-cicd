
  create or replace   view ETL_TESTING.STAGING_STAGING.stg_fda_outc
  
  
  
  
  as (
    select *
from ETL_TESTING.FDA.outc
  );

