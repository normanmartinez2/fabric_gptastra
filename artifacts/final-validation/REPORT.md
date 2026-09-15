# Final Medallion validation — PASSED

Validated 2026-09-15 in DEV, My workspace (`36ef6570-f84b-4d04-8131-7f403f4a3f04`), Lakehouse `lh_medallion_demo` (`8429b9a8-1382-4024-b57d-f9afaeaddb25`), using config/dev.json.

## Deployed resources and flow

- Pipeline `pl_medallion_github` (`38a25222-399d-4bac-8677-04a2acd2ddc4`) exists; its definition matches the validated deployed configuration.
- `Copy_airports_to_bronze`, `Copy_passengers_to_bronze`, and `Copy_bookings_to_bronze` each feed `Notebook_Bronze_to_Silver` on **Succeeded**. All three success dependencies are present.
- `Notebook_Bronze_to_Silver` invokes `nb_silver_bookings_enriched` (`5db26a98-d8ae-40e2-86a0-9ce609a1a8be`).
- `Notebook_Silver_to_Gold` invokes `nb_gold_bookings_by_city` (`a13d249b-0485-41d7-ab6f-e758144f0e26`) and depends exclusively on Silver **Succeeded**.
- Both deployed Notebook definitions were read back: approved code and default Lakehouse bindings match.
- Existing reusable `github_raw_normanmartinez2` connection (`cd43e245-ee02-48cc-9520-42ebb3595721`) remains HTTP/Anonymous with base URL `https://raw.githubusercontent.com/normanmartinez2/`. Binary Copy sinks use existing inline Lakehouse references. Notebooks use the existing Lakehouse context. No additional connection is required.

## Execution

Exactly one new end-to-end run was triggered: `71bc7df1-0930-481a-801f-394692d1038c`. Started **11:04:17 UTC**, finished **11:16:08 UTC**. Fabric pipeline API status: **Completed**, failureReason: **null**. Both Notebook Spark sessions: **Succeeded**. Gold was submitted after Silver completed.

## Data validation

| Output | Format | Rows |
|---|---|---:|
| Files/bronze/airports/airports.csv | CSV | 50 |
| Files/bronze/passengers/passengers.csv | CSV | 200 |
| Files/bronze/bookings/bookings.csv | CSV | 1,000 |
| Files/silver/bookings_enriched/ | Parquet | 1,000 |
| Files/gold/bookings_by_city/ | Parquet | 50 |

Bronze bytes match the previously validated source snapshot. Silver's full joined content matches Bronze; keys are unique in dimensions and every booking joins successfully. Silver schema: booking_id STRING, passenger_id STRING, flight_id STRING, airport_id STRING, amount DECIMAL(18,2), booking_date DATE, passenger_name STRING, passenger_gender STRING, passenger_nationality STRING, airport_name STRING, city STRING, airport_country STRING.

Gold schema is exactly **city STRING, booking_count BIGINT** (booking_count non-nullable). Every city aggregate matches Silver. Eligible Silver rows: **1,000**; excluded null/blank cities: **0**; **sum(booking_count) = 1,000**.

Full logical row multisets for Silver and Gold match the previous successful run. Duplicate Silver booking IDs: **0**; duplicate Gold cities: **0**. Deterministic overwrite paths therefore preserve outputs without accumulating duplicate data. Parquet part filenames are regenerated; logical content, rather than binary file identity, was compared. Counts are regression expectations for the current validated data, not deployed business rules.

## Warnings and scope

No implementation changes, new Fabric resources, or connections were needed. Spark startup introduced queue time. A local validation assertion initially assumed nullable booking_count; inspection confirmed the same non-nullable BIGINT as the prior output, and the assertion was corrected. All final checks passed; no remaining blockers.

Evidence: `run-status.json`, `silver-sessions.json`, `gold-sessions.json`, both `*-notebook-verification.json` files, `data-validation.json`, OneLake listings and downloaded CSV/Parquet. Local validator: `scripts/validate_final_outputs.py`.
