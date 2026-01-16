# ETL Pipeline Testing

An ETL pipeline project focused on **testing and validation**, designed to mirror how data pipelines are structured and tested in real-world cloud environments.  
This project showcases a **full A→Z cloud workflow**: extract FAERS data, transform, validate (Great Expectations), and optionally load to Snowflake — all automated via **CI/CD in GitHub Actions** and reproducible in **Codespaces**.

---
## Demo

Click the button below to **open a Codespace** and run the ETL pipeline automatically (limited to Q1 for demo purposes):

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/masabai/etl-pipeline-testing/codespaces/new)






> ⚠️ **Note:** This demo pipeline runs on the **dev branch** and will extract, transform, and optionally load data. Depending on your network and container spin-up, it may take several minutes to complete.

---

## Features

- Unit, feature, integration, and performance tests for ETL
- Automated validation using **Great Expectations**
- CI/CD via **GitHub Actions**
- Cloud-first design: no local setup required
- Snowflake optional load for production testing
- All scripts reproducible in **GitHub Codespaces**

---

## Quick Start

1. Click the **Demo button** above to open a Codespace.  
2. The ETL pipeline will start automatically via `postCreateCommand` in `.devcontainer/devcontainer.json`.  
3. Explore `data/raw` and `data/processed` folders for input/output files.  
4. Optional: Run tests via:

```bash
pytest tests/
