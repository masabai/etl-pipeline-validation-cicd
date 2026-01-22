{{ config(materialized='table') }}

with base as (

    select
        primaryid,
        caseid,
        caseversion,
        age,
        sex,
        reporter_country,
        occr_country,
        row_number() over(partition by primaryid, caseid order by caseversion) as patient_sk
    from {{ ref('clean_fda_demo') }}

)

select *
from base
