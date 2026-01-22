# ETL Pipeline Testing

A cloud-first ETL pipeline project focused on **testing, validation, and data modeling**, designed to mirror real-world workflows in modern cloud environments.  
This pipeline extracts FDA FAERS datasets, transforms and validates them using **Python, dbt, and Great Expectations**, and optionally loads curated tables to Snowflake (not included for public safety).  

---

## Data Sources & Workflow

- **FDA FAERS**: Multiple quarterly TXT files merged and normalized in Python before warehouse ingestion. Validated upstream with **Great Expectations**, then modeled downstream in dbt.  


### Workflow
1. Extract → Transform → Validate → Load (optional, requires private credentials)  
2. Automated testing: unit, feature, integration, and performance tests  
3. CI/CD pipeline ensures reproducible, production-ready runs  

---

## Engineering Highlights

- **High-volume processing**: Engineered ETL to handle 11.5M+ FDA rows. Chunked loading ensures memory efficiency (8GB RAM).  
- **Performance Benchmark**: Successfully ingested and validated 11.5M+ rows in ~18 minutes using chunked processing.  
- **Security & Connectivity**: Pipeline designed to be cloud-ready without exposing any credentials.  
- **Resilient Transformations**: Sampled validation suites reduce runtime while preserving integrity checks.  

---

## Testing & Validation

- Automated data validation using **Great Expectations**  
- dbt tests, including unique combinations, not null, and accepted values  
- **Example dbt validation:** `clean_fda_demo` flagged 131 duplicate `primaryid + caseversion` combinations out of 11.5M rows, demonstrating that the tests correctly identify data integrity issues.  
- Pytest for pipeline unit and integration tests  
- CI/CD ensures all tests run automatically on commit

> Note: For demo purposes, nulls in staging tables are preserved. In production, these would be handled according to business rules.


## Screenshots

> ![Pipeline Running](screenshots/pipeline_run.png)  
> ![dbt Tests Passing](screenshots/dbt_tests.png)  

- Screenshots show pipeline execution, data transformations, and tests passing.  
- No credentials or cloud access required to view results.  

---


```bash
pytest tests/
