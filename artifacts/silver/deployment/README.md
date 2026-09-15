# Silver deployment and validation

Notebook `nb_silver_bookings_enriched` (`5db26a98-d8ae-40e2-86a0-9ce609a1a8be`) was created with its actual ipynb definition, sourced from the approved local notebook. Only deployment metadata was added to bind the existing `lh_medallion_demo` as default Lakehouse. Readback confirms identical code cells and the intended workspace/Lakehouse binding.

Pipeline `pl_medallion_github` (`38a25222-399d-4bac-8677-04a2acd2ddc4`) now contains `Notebook_Bronze_to_Silver`, with Succeeded dependencies on all three Bronze copies. Readback matched the intended definition. The existing Bronze activities were preserved without configuration changes. No new storage or execution connection was created.

Pipeline run `2144c5bd-3038-4071-9d12-58f75afaa6d3` completed successfully with no failure reason. It ran from 2026-09-15T05:27:19.9239058Z to 2026-09-15T05:31:44.7666667Z. The notebook performed strict input checks, joins, overwrite, and Parquet readback checks. Notebook session evidence is recorded in `notebook.sessions.json`.

Output: `Files/silver/bookings_enriched/`, Parquet, 1,000 rows. Independent PyArrow validation checked every output row against the expected join of the freshly downloaded Bronze datasets: bookings 1,000, passengers 200, airports 50. All before/after Bronze bytes match; the normal pipeline copies refreshed their file metadata, but their contents remain identical. No direct Bronze writes or deletions were performed.

Schema: booking_id string, passenger_id string, flight_id string, airport_id string, amount decimal(18,2), booking_date date, passenger_name string, passenger_gender string, passenger_nationality string, airport_name string, city string, airport_country string. City is sourced from airports.

Evidence:

- `notebook.intended.ipynb`, `notebook.deployed.ipynb`, `notebook-verification.json`: code and Lakehouse readback.
- `pipeline.before.json`, `pipeline.intended.json`, `pipeline.deployed.json`: pipeline comparison.
- `pipeline.run-status.json`: final run result.
- `bronze-preservation.json`: source content preservation.
- `silver-validation.json`: schema, counts, hashes, and full join comparison.
- `bronze-before/`, `bronze-after/`, `parquet/`: downloaded validation inputs and output.

Resolved issues: expired Data Factory token refreshed through interactive authentication; REST operation polling corrected to use the official public endpoint with Fabric's returned operation ID rather than rejecting its regional Location URL. Notebook deployment itself succeeded on the first request. Local PyArrow was installed under `.validation-deps/` for independent validation. No unresolved errors remain.

Deployment used the Notebook REST fallback because available MCP tools do not deploy Notebook definitions; pipeline operations and OneLake reads used the validated MCPs. `scripts/deploy_silver_notebook.py` is scoped to this Notebook and validates the DEV workspace before writes. `scripts/validate_silver_output.py` performs local-only output validation. Refresh discovery and downloads before rerunning these helpers; saved artifacts are evidence for this run.
