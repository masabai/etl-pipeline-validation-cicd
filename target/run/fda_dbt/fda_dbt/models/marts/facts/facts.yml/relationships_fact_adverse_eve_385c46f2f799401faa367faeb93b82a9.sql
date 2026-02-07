
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select drug_sk as from_field
    from ETL_TESTING.STAGING_MARTS.fact_adverse_events
    where drug_sk is not null
),

parent as (
    select drug_sk as to_field
    from ETL_TESTING.STAGING_MARTS.dim_drug
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test