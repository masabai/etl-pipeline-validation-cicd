
with drug_counts as (
    select 
        drug_sk, 
        count(distinct caseid) as num_cases
    from {{ ref('fact_adverse_events') }}
    group by 1
)
select 
    d.drugname, 
    f.num_cases
from drug_counts f
join {{ ref('dim_drug') }} d on f.drug_sk = d.drug_sk
order by num_cases desc
limit 10
