
    
    

with all_values as (

    select
        sex as value_field,
        count(*) as n_records

    from ETL_TESTING.STAGING_CLEAN.clean_fda_demo
    group by sex

)

select *
from all_values
where value_field not in (
    'M','F','UNK','Unknown'
)


