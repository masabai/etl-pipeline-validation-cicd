# ETLPipelineTesting

A testing-focused ETL pipeline project designed to demonstrate **how production data pipelines are validated, tested, and deployed**, not just built.

### What this project does

* Extracts FDA FAERS ASCII (TXT) data directly from the source
* Transforms and merges quarterly datasets into clean, analytics-ready tables
* Applies data quality validation using Great Expectations (schema, nulls, row counts)
* Loads processed data into Snowflake (toggleable to keep CI/CD safe)
* Automates the pipeline using GitHub Actions and GitHub Codespaces

### Testing focus

* Unit tests for extract and transform logic
* Feature tests validating business-level expectations
* Integration tests across extract → transform → load
* Performance tests to detect regressions on large datasets

**Goal:** showcase real-world data engineering practices around **ETL testing, validation, and CI/CD**, aligned with production workflows.
