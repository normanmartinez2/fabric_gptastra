# Silver design proposal — local only

Proposed Notebook item: `nb_silver_bookings_enriched`, PySpark, in `My workspace` (`36ef6570-f84b-4d04-8131-7f403f4a3f04`). The item has not been created. Source: `nb_silver_bookings_enriched.ipynb`; rebuild with `python scripts/build_silver_design.py`. The builder profiles downloaded Bronze CSVs and checks Python syntax; it does not run Spark or write to Fabric.

## Discovered inputs and contract

Live OneLake files were listed and freshly downloaded with Fabric Local MCP before notebook generation. `bronze-profile.json` contains full headers, counts, nulls, duplicate-key counts, hashes, and type checks; `mcp-evidence.json` records the read operations. CSV has no stored physical types. The observed values support these explicit Spark input types:

| Dataset | Rows | Schema |
|---|---:|---|
| bookings | 1,000 | booking_id string, passenger_id string, flight_id string, airport_id string, amount decimal(18,2), booking_date date |
| passengers | 200 | passenger_id string, name string, gender string, nationality string |
| airports | 50 | airport_id string, airport_name string, city string, country string |

All columns have zero null/blank values. Booking IDs and both dimension keys are unique. No join-key whitespace or unmatched bookings was found. Bookings contain 200 distinct passenger IDs and 50 distinct airport IDs; repeated foreign keys are expected. Both joins are many-to-one and should preserve all 1,000 bookings. Amounts have at most two fractional digits and range from 102.97 to 1499.02; dates parse as yyyy-MM-dd and range from 2025-03-24 to 2025-06-21. IDs contain letter prefixes and must remain strings.

## Notebook behavior

Attach the existing `lh_medallion_demo` (`8429b9a8-1382-4024-b57d-f9afaeaddb25`) as the notebook's **default Lakehouse**, in the same DEV workspace. Attachment is a future deployment step, not embedded as an invented metadata schema. The runtime guard refuses a missing or different Lakehouse/workspace. Relative Spark paths address the default Lakehouse; strict CSV preflight reads the same inputs through `/lakehouse/default/`. No new Lakehouse is needed.

The generated parameter cell holds `config_json`, sourced from `config/dev.json`. A separate generated snapshot and the verified Lakehouse ID prevent unreviewed workspace/path overrides. The repository is not assumed to be available inside Fabric. Regenerate from current config and refreshed discovery before deployment; do not hand-enter copies of environment settings.

The notebook checks normalized headers for missing/extra/reordered columns and collisions, validates every record's width, validates decimal/date representability, and uses explicit StructType schemas with FAILFAST parsing. It refuses empty inputs or files larger than 10 MiB, a deliberate bound for this small lab. These bounds require review for larger datasets.

Null/blank booking IDs and join keys, duplicate booking/dimension keys, whitespace-padded keys, or orphan bookings cause failure before output is touched. Repeated foreign keys on bookings are accepted and reported. No automatic deduplication or orphan dropping occurs. Other nullable attributes retain their nulls; missing/blank airport city fails the Gold-readiness check. Column names are normalized to lowercase snake_case; data values are preserved rather than silently trimmed or case-folded.

Use validated inner joins on passenger_id and airport_id. Rename passenger attributes before joining, and use key-list joins to emit keys once. The explicit final projection is:

`booking_id, passenger_id, flight_id, airport_id, amount, booking_date, passenger_name, passenger_gender, passenger_nationality, airport_name, city, airport_country`.

`city` means **airports.city**. The first six columns retain the booking schema; the six descriptive columns are strings. Each join must preserve booking count and final booking IDs must remain unique.

Write Parquet with `mode('overwrite')` to the configured deterministic path `Files/silver/bookings_enriched/`. Recheck the runtime workspace/Lakehouse and Bronze hashes immediately before the write. Read the written Parquet back and compare schema, row count, and bidirectional multiset content. Only then print `status: Succeeded` together with all input counts, Silver count, null/duplicate/orphan metrics, and output schema. Exceptions propagate to fail the Notebook activity.

Overwrite is repeatable for unchanged inputs, but ordinary Parquet overwrite is not a transactional publication mechanism. Assume one writer/run at a time and no concurrent Bronze refresh; serialize future pipeline runs operationally. Failed writes/readback can leave partial output, so downstream work must depend on Notebook success. No concurrent-run protection or Fabric execution has been validated in this design phase.

## Pipeline integration proposal

Preserve the existing three Bronze activities byte-for-byte. Append one activity named `Notebook_Bronze_to_Silver` with type `TridentNotebook` to `pl_medallion_github` after their convergence:

```json
{
  "name": "Notebook_Bronze_to_Silver",
  "type": "TridentNotebook",
  "dependsOn": [
    {"activity": "Copy_airports_to_bronze", "dependencyConditions": ["Succeeded"]},
    {"activity": "Copy_passengers_to_bronze", "dependencyConditions": ["Succeeded"]},
    {"activity": "Copy_bookings_to_bronze", "dependencyConditions": ["Succeeded"]}
  ]
}
```

This is a dependency fragment, **not a deployable definition**: at future deployment add required `typeProperties.notebookId` from the created/discovered Notebook and `typeProperties.workspaceId` from config. No Notebook ID is invented. Use the generated parameter defaults or pass the same config JSON through the Notebook activity's base parameters. No retries are proposed for deterministic data validation failures. A later Gold activity must depend on `Notebook_Bronze_to_Silver` with `Succeeded`; it is outside this change.

Current pipeline readback equals the validated Bronze definition (`pipeline.current.json`). At deployment, discover any existing Notebook first, re-read the pipeline to preserve concurrent edits, validate workspace IDs, create/update only the intended Notebook, attach the existing Lakehouse, and verify Notebook and pipeline readbacks. Confirm the three dependency edges and that a failed Bronze activity prevents Silver.

## Connections and future validation

No HTTP or Lakehouse Data Factory connection is required for the notebook's Spark storage access. Fabric runs the notebook under its execution identity, which must have access to the existing Lakehouse. The current Fabric item schema marks `TridentNotebook.externalReferences` optional, so there is no demonstrated requirement to create a new connection. Current UI documentation exposes a Notebook activity authentication **Connection** setting; the actual execution-identity binding must be verified at deployment. Reuse a suitable existing binding if needed; do not assume a new connection or Workspace Identity is necessary, especially in this Personal workspace.

Future authorized execution should verify runtime identity/access, Spark schema behavior, the initial 1,000-row result, Parquet readback, and a second overwrite run with identical content and no accumulation. Exercise malformed/missing columns, invalid decimal/date, null keys, duplicate dimension/booking keys, orphan keys, and wrong context in isolated test fixtures; each must fail before the write. No such fixtures should overwrite the validated Bronze inputs. Only source profiling and syntax/static checks were performed now; the Silver transformation was not deployed or executed.

## Retrieved references

- Fabric Local `docs_item_definitions(notebook)`: supports ipynb and FabricGitSource PySpark notebook sources; saved in MCP evidence.
- Fabric Local `docs_item_definitions(dataPipeline)`: required Notebook/workspace IDs, optional parameters/connection reference; saved excerpt in `notebook-activity-schema.md`.
- [Fabric Lakehouse notebook access](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-notebook-load-data): default Lakehouse relative Spark paths and mounted file access.
- [NotebookUtils runtime context](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-runtime): runtime workspace and default Lakehouse identity guards.
- [Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity): pipeline invocation, parameter cells, and authentication selection.
- [Spark CSV options](https://spark.apache.org/docs/latest/sql-data-sources-csv.html): explicit schemas, parser options, and header enforcement.
