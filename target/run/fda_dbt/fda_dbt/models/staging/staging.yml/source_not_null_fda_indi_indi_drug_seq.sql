
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select indi_drug_seq
from ETL_TESTING.FDA.indi
where indi_drug_seq is null



  
  
      
    ) dbt_internal_test