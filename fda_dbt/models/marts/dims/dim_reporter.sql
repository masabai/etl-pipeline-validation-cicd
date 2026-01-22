{{ config(materialized='table') }}

with base as (

    select
        primaryid,
        caseid,
        rpsr_cod,
        row_number() over(partition by primaryid, caseid order by rpsr_cod) as rpsr_sk
    from {{ ref('clean_fda_rpsr') }}

)

select *
from base
