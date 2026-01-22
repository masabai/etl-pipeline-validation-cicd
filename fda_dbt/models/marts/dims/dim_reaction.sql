{{ config(materialized='table') }}

with base as (

    select
        primaryid,
        caseid,
        pt,
        row_number() over(partition by primaryid, caseid order by pt) as reac_sk
    from {{ ref('clean_fda_reac') }}

)

select *
from base
