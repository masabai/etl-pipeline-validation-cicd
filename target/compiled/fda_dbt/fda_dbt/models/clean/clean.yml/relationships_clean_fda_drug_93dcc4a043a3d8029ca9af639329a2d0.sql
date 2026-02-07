
    
    

with child as (
    select primaryid as from_field
    from ETL_TESTING.STAGING_CLEAN.clean_fda_drug
    where primaryid is not null
),

parent as (
    select primaryid as to_field
    from ETL_TESTING.STAGING_CLEAN.clean_fda_demo
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


