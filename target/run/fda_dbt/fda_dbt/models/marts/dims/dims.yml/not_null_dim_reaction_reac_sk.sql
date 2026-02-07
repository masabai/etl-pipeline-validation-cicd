
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reac_sk
from ETL_TESTING.STAGING_MARTS.dim_reaction
where reac_sk is null



  
  
      
    ) dbt_internal_test