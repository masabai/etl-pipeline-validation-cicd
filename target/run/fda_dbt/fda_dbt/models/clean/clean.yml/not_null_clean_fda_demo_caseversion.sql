
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select caseversion
from ETL_TESTING.STAGING_CLEAN.clean_fda_demo
where caseversion is null



  
  
      
    ) dbt_internal_test