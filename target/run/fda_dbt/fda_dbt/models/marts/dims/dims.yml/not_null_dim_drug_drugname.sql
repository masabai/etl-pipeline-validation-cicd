
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select drugname
from ETL_TESTING.STAGING_MARTS.dim_drug
where drugname is null



  
  
      
    ) dbt_internal_test