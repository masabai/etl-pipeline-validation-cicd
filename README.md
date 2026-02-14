# ETL Pipeline Testing

This project is a cloud-based ETL pipeline for **FDA FAERS datasets**.  
It extracts quarterly raw TXT files, transforms and validates them using **Python, dbt, and Great Expectations**, and loads cleaned tables into Snowflake.  

A **Snowflake-native Streamlit dashboard** is included to show in-warehouse visualizations. Only a small set of validated metrics is displayed to ensure accuracy and avoid heavy queries.

graph LR
    A[data.gov] --> B(Extract & Transform)
    B --> C{Validate: GX}
    C -->|Pass| D[(Snowflake)]
    D --> E(dbt Models)
    E --> F[Streamlit Dashboard]
    
    subgraph Automation
    G[GitHub Actions] -.-> B
    G -.-> E
    end

    style F fill:#ff4b4b,stroke:#333,color:#fff
    style D fill:#29b5e8,stroke:#333,color:#fff
    style G fill:#24292e,stroke:#333,color:#fff






**Workflow at a glance:**  
Extract (data.gov) → Transform (Pandas) → Validate (GX/Pytest) → Load (Snowflake) → Model (dbt) → Visualize (Streamlit) → Automate (GitHub Actions) 

---

## Workflow

1. Extract raw FAERS TXT files.  
2. Transform and merge files into structured CSVs.  
3. Validate processed data using **Great Expectations**.  
4. Optionally load cleaned tables to Snowflake (requires credentials).  
5. Run dbt models and tests locally.  
6. CI/CD ensures automated testing and reproducibility.

---

## Engineering Highlights

- Processed **11.5M+ rows** from FAERS datasets using chunked loading to avoid memory issues.  
- End-to-end workflow (extract → transform → validate → load → dbt models/tests) completed in ~14 minutes with **58 dbt models** and **24 tests**.  
- Pipeline runs securely without storing credentials in code.  

---

## Testing & Validation

- **Great Expectations** validates row counts and expected columns.  
- dbt tests check for duplicates, nulls, and accepted values.  
- Example: `clean_fda_demo` flagged 131 duplicate `primaryid + caseversion` combinations out of 11.5M rows.  
- Unit tests implemented with **pytest**.  
- CI/CD ensures all tests run automatically on commit.

> Note: For demonstration, nulls in staging tables are preserved. In production, these would be handled according to business rules.

---

## Streamlit Dashboard

A Snowflake-native **Streamlit dashboard** demonstrates interactive visualizations of FAERS data for top antidepressants:

- **Drugs included:** SERTRALINE, FLUOXETINE, CITALOPRAM, ESCITALOPRAM, VENLAFAXINE, DULOXETINE, BUPROPION, TRAZODONE, AMITRIPTYLINE  
- **Time period:** 2025 Q1–Q2 (6 months of data)  
- **Outcomes highlighted:** Death (DE), Life-Threatening (LT), Hospitalization (HO), Disability (DS)  
- Multi-tab, multi-color bar charts for clear distinction  
- Raw data tables displayed below charts for context and verification  

This lightweight demo showcases **in-warehouse analytics** using Snowflake’s Streamlit integration without heavy queries.

---

## Screenshots

> ![Pipeline Running](screenshots/pipeline_run.png)  
> ![dbt Tests Passing](screenshots/dbt_tests.png)  

- Screenshots show pipeline execution, data transformations, and test results.  
- No credentials or cloud access are required to view results.

---

## Running Tests

```bash
pytest tests/
