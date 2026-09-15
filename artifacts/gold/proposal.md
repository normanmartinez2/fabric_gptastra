# Gold proposal — not deployed or executed

Notebook name: `nb_gold_bookings_by_city`. Local source: `nb_gold_bookings_by_city.ipynb`. Rebuild using `python scripts/build_gold_design.py`; this checks syntax but does not execute Spark. Environment defaults and binding metadata are generated from `config/dev.json` and the previously verified target Lakehouse discovery. Runtime guards compare the actual Notebook/default Lakehouse workspace and ID before reading and immediately before writing.

## Observed Silver input

Fresh Fabric Local OneLake listing/download confirmed the deployed Parquet under `Files/silver/bookings_enriched/`. `silver-profile.json` records its actual 12-column schema and 1,000 rows. Types are string except amount decimal(18,2) and booking_date date. Columns: booking_id, passenger_id, flight_id, airport_id, amount, booking_date, passenger_name, passenger_gender, passenger_nationality, airport_name, city, airport_country.

Exactly one city field exists: `city`, string. Its authoritative lineage is airports.city through the validated deployed Silver notebook's explicit projection. No competing city field exists. Observed city nulls: 0; whitespace-only values: 0; padded values: 0; distinct nonblank cities: 50. Missing/ambiguous city or full Silver schema drift causes the proposed notebook to stop before any write.

## Business and validation rules

- Eligible rows have a non-null city containing at least one non-whitespace character. Exclude null and Unicode-whitespace-only city values, report each category, and reconcile input = eligible + excluded. This exclusion policy is explicit; no unknown-city bucket is invented.
- Preserve valid city labels exactly, including case and any surrounding whitespace. No city merging or trimming is applied. SQL-style grouping uses the Silver city value as stored.
- Require non-null/nonblank unique booking_id so counting rows counts bookings. Group by city and use count(*) semantics, not distinct city or passenger counts.
- Output contains exactly `city STRING` and `booking_count BIGINT` (Spark LongType), in that order. Validate positive/non-null counts, unique/nonblank cities, output groups = eligible distinct cities, and sum(booking_count) = eligible rows.
- Expected metrics for inspected data: input 1,000; eligible 1,000; excluded 0; output groups 50; booking_count sum 1,000. These are expectations from Silver profiling, not an executed Gold result. Runtime reports actual input, eligible, excluded, null, blank, and output counts.
- If no eligible rows exist, fail before overwrite and report eligibility metrics. Do not replace a valid prior aggregate with an empty batch silently.
- Overwrite Parquet at deterministic `Files/gold/bookings_by_city/` in the same `lh_medallion_demo` Lakehouse. Validate the written Parquet schema, group count, count sum, and exact multiset content by reading it back before reporting Succeeded.

Assume one writer at a time and no concurrent Silver overwrite. Ordinary Parquet overwrite is repeatable but not transactional; failed writes may leave partial output. The Notebook must fail on write/readback errors, and consumers must require successful completion. No business exclusions other than invalid city are applied.

## Pipeline integration (proposal only)

Preserve all existing activities and configurations in `pl_medallion_github`. Append `Notebook_Silver_to_Gold` with type `TridentNotebook`, invoking the intended Notebook above using its discovered/created ID at future deployment. Set exactly this dependency:

```json
"dependsOn": [
  {"activity": "Notebook_Bronze_to_Silver", "dependencyConditions": ["Succeeded"]}
]
```

Required typeProperties: notebookId from future Notebook discovery/creation; workspaceId from config. Use the generated config_json parameter defaults (or pass the same reviewed config). No invented Notebook ID is included. No retries are proposed for deterministic validation errors. Silver failure/skipping must prevent Gold from executing.

Use the existing `lh_medallion_demo` as default Lakehouse for both input and output. The Notebook artifact includes the same supported Lakehouse binding metadata already validated during Silver deployment. No new storage connection is required. Silver demonstrated successful pipeline Notebook execution under the existing user identity without an added connection; reuse that execution pattern.

At future authorized deployment, re-read AGENTS/config, refresh Notebook/pipeline discovery and Silver schema, preserve existing definitions, deploy actual Notebook code and binding, verify readbacks/dependencies, and run validation. Test invalid/missing/ambiguous city fields, null/blank eligibility, duplicate booking IDs, count reconciliation, wrong workspace, and repeated overwrite using isolated fixtures; do not modify the validated upstream datasets. Runtime and idempotency checks remain pending because Gold execution is not authorized in this task.

## References and evidence

- `silver-profile.json`: fresh schema/count/city profiling.
- `silver-listing.json`: live OneLake paths, sizes, timestamps, and ETags.
- Existing `artifacts/silver/notebook-activity-schema.md`: retrieved Fabric TridentNotebook definition.
- Existing `artifacts/silver/deployment/notebook-verification.json`: validated default-Lakehouse binding pattern.
- [Fabric Notebook Lakehouse access](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-notebook-load-data).
- [Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity).

Only local artifacts and read-only inspection were performed. No Fabric Notebook, pipeline, connection, Bronze, Silver, or Gold data was created or modified.
