{{ config(materialized='table') }}

with base as (

    select
        primaryid,
        caseid,
        outc_cod,
        row_number() over(partition by primaryid, caseid order by outc_cod) as outc_sk
    from {{ ref('clean_fda_outc') }}

)

select *
from base
