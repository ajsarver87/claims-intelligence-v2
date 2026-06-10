# Claims Intelligence v2

A data pipeline for ingesting and transforming CMS Medicare claims data using Dagster and dbt, with BigQuery as the data warehouse.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- GCP service account with BigQuery and GCS permissions
- BigQuery dataset created (e.g., `dbt_dev` for local development)

## Environment Setup

Create a `.env` file in the project root:

```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account.json
GCP_PROJECT=your-gcp-project-id
GCP_PROJECT_LOCATION=us-central1
GCP_RAW_DATASET=dataset_to_land_raw_data
GCP_RAW_TABLE=table_to_land_raw_data
GCP_RAW_STAGING_TABLE_BASE=your_staging_table_prefix
GCS_BUCKET=storage-bucket-name
```

## Getting Started

Install dependencies:

```bash
uv sync
```

Install dbt packages:

```bash
cd src/claims_intelligence/claims_intelligence_dbt
uv run dbt deps
```

Start the Dagster UI:

```bash
uv run dg dev
```

The Dagster webserver will be available at http://localhost:3000.

## Project Structure

```
src/claims_intelligence/
├── definitions.py                  # Dagster entrypoint
├── defs/
│   ├── assets/
│   │   └── ingestion/              # Raw ingestion assets (CMS Part D, etc.)
│   ├── resources.py                # BigQuery and GCS resource definitions
│   └── transform/
│       └── defs.yaml               # dbt project component
└── claims_intelligence_dbt/        # dbt project
    ├── models/
    │   └── silver/                 # Transformed models
    ├── dbt_project.yml
    ├── packages.yml
    └── profiles.yml
```

## dbt

To run dbt commands directly, navigate to the dbt project directory first:

```bash
cd src/claims_intelligence/claims_intelligence_dbt
uv run dbt debug       # verify connection
uv run dbt build       # run and test all models
uv run dbt run         # run models only
uv run dbt test        # run tests only
```
