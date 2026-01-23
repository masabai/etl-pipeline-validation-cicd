{{ config(
    materialized='table',
    schema='EDA'
) }}

with drug_deaths as (
    select
        f.drug_sk,
        f.caseid
    from {{ ref('fact_adverse_events') }} f
    join {{ ref('dim_outcome') }} o
        on f.outc_sk = o.outc_sk
    where o.outc_cod = 'DE'  -- Death
    group by f.drug_sk, f.caseid
)

select
    d.drugname,
    'DE' as outc_cod,
    count(*) as serious_cases
from drug_deaths dd
join {{ ref('dim_drug') }} d
    on dd.drug_sk = d.drug_sk
group by d.drugname
order by serious_cases desc
limit 10
