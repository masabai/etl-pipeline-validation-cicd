
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select caseid
from ETL_TESTING.STAGING_CLEAN.clean_fda_indi
where caseid is null



  
  
      
    ) dbt_internal_test