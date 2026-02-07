
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select primaryid
from ETL_TESTING.FDA.ther
where primaryid is null



  
  
      
    ) dbt_internal_test