# Microsoft Fabric Medallion Project

A data engineering solution that ingests CSV files from GitHub, enriches bookings with passenger and airport information, and produces a booking aggregate by city. Implemented with Codex on Microsoft Fabric and validated end to end in the **DEV** environment.

## Architecture

```text
GitHub: airports.csv, passengers.csv and bookings.csv
                        |
           Reusable HTTP connection
                        |
           Bronze: original CSV files
                        |
        Silver: enriched bookings (Parquet)
                        |
        Gold: bookings by city (Parquet)
```

**Fabric Data Factory** orchestrates the process through `pl_medallion_github`. **PySpark** notebooks perform the transformations, and **OneLake**, through `lh_medallion_demo`, stores all three layers.

Environment settings, sources, and paths are centralized in [config/dev.json](config/dev.json). Development and operational rules are documented in [AGENTS.md](AGENTS.md). The project exclusively uses the DEV workspace **My workspace**.

## Data Layers

### Bronze: ingestion without transformations

Three Copy activities download the CSV files from the [databricks_claude](https://github.com/normanmartinez2/databricks_claude) repository:

- `Copy_airports_to_bronze`
- `Copy_passengers_to_bronze`
- `Copy_bookings_to_bronze`

All activities use the reusable HTTP connection `github_raw_normanmartinez2`, with anonymous authentication and the base URL `https://raw.githubusercontent.com/normanmartinez2/`. **Binary → Binary** copying preserves the original CSV content.

### Silver: validation and enrichment

The `nb_silver_bookings_enriched` notebook reads Bronze files using explicit schemas, normalizes column names, and validates required columns, null keys, duplicates, and join cardinality. It stops processing when unsafe conditions are detected.

It enriches each booking through these relationships:

- `bookings.passenger_id = passengers.passenger_id`
- `bookings.airport_id = airports.airport_id`

An explicit projection prevents ambiguous columns and produces `booking_id`, `passenger_id`, `flight_id`, `airport_id`, `amount`, `booking_date`, `passenger_name`, `passenger_gender`, `passenger_nationality`, `airport_name`, `city`, and `airport_country`. All types are STRING except `amount` DECIMAL(18,2) and `booking_date` DATE.

### Gold: bookings by city

The `nb_gold_bookings_by_city` notebook verifies the Silver schema and uses its `city` field, which originates from airports. It groups bookings by city and produces exactly:

| Column | Type | Description |
|---|---|---|
| `city` | STRING | Airport city |
| `booking_count` | BIGINT | Number of bookings for the city |

Null or whitespace-only city values are excluded; all other labels remain unchanged. The sum of `booking_count` is checked against the eligible Silver row count.

Silver and Gold write Parquet using **overwrite** at deterministic paths and validate the output by reading it back. Both notebooks use the existing Lakehouse as their context; no additional storage connection is required.

## Orchestration

The three Bronze activities can run independently. `Notebook_Bronze_to_Silver` invokes the Silver notebook only when **all three Copy activities finish with Succeeded**. Then, `Notebook_Silver_to_Gold` invokes the Gold notebook only when **Silver finishes with Succeeded**.

The entire flow runs through the single pipeline `pl_medallion_github`. The deployed notebooks contain the code from the local artifacts; their definitions and Lakehouse bindings were verified by reading them back from Fabric.

## Validated Results

The final validation on **September 15, 2026** confirmed a complete run without failures: pipeline status **Completed** and both notebooks **Succeeded**.

| Layer / Dataset | Lakehouse Path | Format | Rows |
|---|---|---|---:|
| Bronze / airports | `Files/bronze/airports/airports.csv` | CSV | 50 |
| Bronze / passengers | `Files/bronze/passengers/passengers.csv` | CSV | 200 |
| Bronze / bookings | `Files/bronze/bookings/bookings.csv` | CSV | 1,000 |
| Silver / enriched bookings | `Files/silver/bookings_enriched/` | Parquet | 1,000 |
| Gold / bookings by city | `Files/gold/bookings_by_city/` | Parquet | 50 |

The sum of `booking_count` was **1,000**, with **0 rows excluded** for null or blank city values. Schemas, relationships with Bronze, and all city aggregates were verified.

Rerunning the pipeline preserved the logical content of Silver and Gold without duplicating bookings or cities. Parquet part filenames may change between runs. These counts reflect the validated source data and are not fixed business rules.

Execution details and evidence are available in [artifacts/final-validation/REPORT.md](artifacts/final-validation/REPORT.md). No blockers remained.

## Key Files

| Location | Contents |
|---|---|
| [config/dev.json](config/dev.json) | Authorized DEV configuration, sources, and paths |
| [AGENTS.md](AGENTS.md) | Architecture, rules, and validation requirements |
| [artifacts/bronze/](artifacts/bronze/) | Ingestion definitions and evidence |
| [Silver Notebook](artifacts/silver/nb_silver_bookings_enriched.ipynb) | Bronze-to-Silver PySpark transformation |
| [Gold Notebook](artifacts/gold/nb_gold_bookings_by_city.ipynb) | Silver-to-Gold PySpark aggregation |
| [scripts/](scripts/) | Deployment and validation utilities |
| [artifacts/final-validation/](artifacts/final-validation/) | Final report, execution statuses, and data evidence |

## Execution and Verification

1. Confirm the environment and paths in `config/dev.json`.
2. In the configured workspace, run `pl_medallion_github` on demand.
3. Verify that all five activities complete successfully.
4. Validate the Bronze, Silver, and Gold outputs, their schemas, and the reconciliation of eligible bookings with `sum(booking_count)`.

The scripts in `scripts/` are local utilities; transformations execute in Fabric. `validate_final_outputs.py` checks downloaded copies and compares results with the previous run, so it requires up-to-date local evidence and PyArrow.
