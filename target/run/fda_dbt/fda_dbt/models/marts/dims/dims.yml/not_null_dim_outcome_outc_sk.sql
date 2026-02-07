
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select outc_sk
from ETL_TESTING.STAGING_MARTS.dim_outcome
where outc_sk is null



  
  
      
    ) dbt_internal_test