# ETLPipelineTesting

[![ETL CI/CD Workflow](https://github.com/masabai/etl-pipeline-testing/actions/workflows/fda-etl.yml/badge.svg)](https://github.com/masabai/etl-pipeline-testing/actions/workflows/fda-etl.yml)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/masabai/etl-pipeline-testing)

A **cloud-first, CI/CD-driven ETL pipeline** built entirely using **GitHub Actions and GitHub Codespaces**, focused on **testing, validation, and automation** rather than local execution.

### What this project does
- Executes the full ETL workflow in the cloud using GitHub Actions and Codespaces
- Extracts FDA FAERS ASCII (TXT) data directly from the source
- Transforms and merges quarterly datasets into analytics-ready tables
- Validates data quality using Great Expectations (schema, nulls, row counts)
- Loads processed data into Snowflake (toggleable for CI safety)

### Testing focus
- Unit tests for extract and transform logic  
- Feature tests validating dataset-level expectations  
- Integration tests across extract → transform → load  
- Performance tests to observe behavior on large datasets  

**Goal:** showcase an **end-to-end cloud ETL workflow**, demonstrating practical understanding of **CI/CD automation, data testing, and validation** using industry-standard tools.
