
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select outc_cod
from ETL_TESTING.FDA.outc
where outc_cod is null



  
  
      
    ) dbt_internal_test