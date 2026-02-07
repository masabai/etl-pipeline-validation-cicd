
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pt
from ETL_TESTING.FDA.reac
where pt is null



  
  
      
    ) dbt_internal_test