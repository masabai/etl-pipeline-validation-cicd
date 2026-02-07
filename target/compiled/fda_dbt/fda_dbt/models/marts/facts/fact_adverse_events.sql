

with base as (

    select
        p.patient_sk,
        p.primaryid,
        p.caseid,
        p.caseversion,
        p.age,
        p.sex,
        p.reporter_country,
        p.occr_country,
        d.drug_sk,
        r.reac_sk,
        o.outc_sk,
        rp.rpsr_sk
    from ETL_TESTING.STAGING_MARTS.dim_patient p
    left join ETL_TESTING.STAGING_MARTS.dim_drug d
        on p.primaryid = d.primaryid and p.caseid = d.caseid
    left join ETL_TESTING.STAGING_MARTS.dim_reaction r
        on p.primaryid = r.primaryid and p.caseid = r.caseid
    left join ETL_TESTING.STAGING_MARTS.dim_outcome o
        on p.primaryid = o.primaryid and p.caseid = o.caseid
    left join ETL_TESTING.STAGING_MARTS.dim_reporter rp
        on p.primaryid = rp.primaryid and p.caseid = rp.caseid

)

select *
from base