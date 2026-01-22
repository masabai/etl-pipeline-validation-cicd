{{ config(materialized='table') }}

with base as (

    select
        primaryid,
        caseid,
        drug_seq,
        role_cod,
        drugname,
        prod_ai,
        row_number() over(partition by primaryid, caseid, drug_seq order by drug_seq) as drug_sk
    from {{ ref('clean_fda_drug') }}

)

select *
from base
