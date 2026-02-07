
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select drug_seq
from ETL_TESTING.FDA.drug
where drug_seq is null



  
  
      
    ) dbt_internal_test