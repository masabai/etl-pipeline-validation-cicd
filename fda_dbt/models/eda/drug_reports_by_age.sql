-- models/eda/drug_reports_by_age.sql

with counts as (
    select
        f.drug_sk,
        case 
            when f.age >= 65 then 'Elderly'
            when f.age < 18 then 'Pediatric'
            else 'Adult'
        end as age_group,
        count(distinct f.caseid) as report_count
    from {{ ref('fact_adverse_events') }} f
    where f.age is not null
    group by f.drug_sk, age_group
)

select
    d.drugname,
    c.age_group,
    c.report_count,
    rank() over (partition by c.age_group order by c.report_count desc) as rank_within_age_group
from counts c
join {{ ref('dim_drug') }} d
    on c.drug_sk = d.drug_sk
