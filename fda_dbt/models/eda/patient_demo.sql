{{ config(
    materialized='table',
    schema='EDA'
) }}

select 
    age,
    sex,
    count(*) as num_patients
from {{ ref('fact_adverse_events') }}
where age is not null
group by age, sex
order by num_patients desc
