# Bronze implementation evidence

Pipeline `pl_medallion_github` completed successfully on 2026-09-14. Fabric MCP returned terminal status `Completed` with no failure reason for run `5e1bb2be-8227-4820-8d41-d2450589174a`. All three Bronze outputs exist at the configured Lakehouse paths and are byte-identical to freshly downloaded GitHub sources. Source and target row counts: airports 50/50, passengers 200/200, bookings 1000/1000. Headers and SHA-256 hashes match.

The old HTTP connection (`0c0d5d64-2c92-40fc-ab07-a45c35d37827`) was preserved as `github_raw_normanmartinez2_without_trailing_slash`. The replacement `github_raw_normanmartinez2` (`cd43e245-ee02-48cc-9520-42ebb3595721`) uses `https://raw.githubusercontent.com/normanmartinez2/` with Anonymous authentication. The user explicitly authorized this correction. The rename used the official REST API because Data Factory MCP has no rename tool; creation, readback, pipeline update, and execution used Data Factory MCP. No resources were deleted.

Only the three Bronze source connection references changed in the pipeline. Readback matched the intended definition. Each Binary destination uses an inline Lakehouse linked service targeting the verified DEV Lakehouse; no separate Lakehouse connection was created.

- `discovery.json`: discovered target resources and replacement HTTP connection.
- `connection.preserved.json` and `connection.replacement.json`: MCP connection readbacks.
- `pipeline.initial.json`: initial empty pipeline content.
- `pipeline.before.json` and `pipeline.before-rebind.json`: pipeline content before the connection replacement.
- `pipeline.intended.json`: generated Copy activities.
- `pipeline.deployed.json`: definition read back from Fabric and compared structurally with the intended definition.
- `source-validation.json`: parsed CSV headers, row counts, and SHA-256 hashes.
- `validation.json`: source/target comparison after downloading Bronze outputs.
- `run-status.json` and `run-success.json`: final MCP run-status response.
- `run-failed.json`: prior failed run retained for traceability.
- `outputs-mcp.json`: OneLake file discovery and download metadata.
- `source/` and `target/`: original downloaded bytes for validation.

Generate the definition and source report with `python scripts/bronze.py`. After downloading target files through Fabric Local MCP, run `python scripts/bronze.py --validate-target`. The script reads environment settings from `config/dev.json` and discovered IDs from `discovery.json`. Refresh discovery and the current pipeline definition before any subsequent deployment; saved evidence is not live state. The script makes no Fabric writes.

CSV schema labels describe the file representation, not a typed ingestion schema. Binary copy performs no parsing, mappings, compression, or business transformations.

The first run (`d15919e9-25b5-4e44-b59f-571b7a2e7342`) failed with HTTP 404 and a Fabric diagnostic requiring a trailing slash on a base URL containing a path. An attempted dataset-level `connectionProperties.url` override also failed (run `65199c56-d5c9-4af5-8670-e6aa79467129`). The ineffective override was removed. `pipeline.failed.json` records the first deployed definition. Replacement of the shared connection resolved the failure.

Browser discovery returned no available browser. The official Fabric connection-update API, retrieved through Fabric Local and checked against Microsoft Learn, cannot update the connection URL. The user approved the preserved connection and replacement approach recorded in `connection-fix-proposal.json`; that proposal has now been applied.

Documentation retrieved for implementation:

- Fabric Local `docs_item_definitions(dataPipeline)` (saved in `fabric-definition-reference.md`).
- https://learn.microsoft.com/en-us/fabric/data-factory/connector-http-copy-activity
- https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse-copy-activity
- https://learn.microsoft.com/en-us/fabric/data-factory/format-binary
- https://learn.microsoft.com/en-us/azure/data-factory/connector-http
- https://learn.microsoft.com/en-us/azure/data-factory/connector-microsoft-fabric-lakehouse

Authentication initially failed in Data Factory MCP and was resolved using its interactive authentication tool. OneLake listing by friendly name returned HTTP 400; ID-based listing worked. An early Bronze folder lookup returned HTTP 404 while the run was queued. Initial sandboxed source downloads failed; the approved network retry succeeded.
