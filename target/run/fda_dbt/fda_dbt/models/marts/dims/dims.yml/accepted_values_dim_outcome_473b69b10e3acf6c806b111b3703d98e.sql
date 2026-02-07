
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        outc_cod as value_field,
        count(*) as n_records

    from ETL_TESTING.STAGING_MARTS.dim_outcome
    group by outc_cod

)

select *
from all_values
where value_field not in (
    'DE','LT','HO','DS','CA','RI','OT'
)



  
  
      
    ) dbt_internal_test