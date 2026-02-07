
    
    

with child as (
    select rpsr_sk as from_field
    from ETL_TESTING.STAGING_MARTS.fact_adverse_events
    where rpsr_sk is not null
),

parent as (
    select rpsr_sk as to_field
    from ETL_TESTING.STAGING_MARTS.dim_reporter
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


