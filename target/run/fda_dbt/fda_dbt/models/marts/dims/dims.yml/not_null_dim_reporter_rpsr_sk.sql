
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rpsr_sk
from ETL_TESTING.STAGING_MARTS.dim_reporter
where rpsr_sk is null



  
  
      
    ) dbt_internal_test