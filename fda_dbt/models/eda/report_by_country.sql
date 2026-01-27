{{ config(
    materialized='table',
    schema='EDA'
) }}

select 
    occr_country,
    count(distinct caseid) as num_reports
from {{ ref('fact_adverse_events') }}
where occr_country is not null
group by occr_country
order by num_reports desc
limit 10
