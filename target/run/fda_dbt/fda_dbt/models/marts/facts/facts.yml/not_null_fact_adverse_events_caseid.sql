
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select caseid
from ETL_TESTING.STAGING_MARTS.fact_adverse_events
where caseid is null



  
  
      
    ) dbt_internal_test