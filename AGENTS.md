# Project: Microsoft Fabric Medallion with Codex / GPT-6 Astra

## Goal

Build a Microsoft Fabric Medallion pipeline using Codex with GPT-6 Astra.

Target architecture:

GitHub CSV
    -> Bronze CSV
    -> Silver Parquet
    -> Gold Parquet

Microsoft Fabric is the execution platform.

OneLake / Lakehouse is the storage layer.

Fabric Data Factory is the orchestration layer.

---

## Target Environment

Environment: DEV only.

Fabric workspace:

- Name: My workspace
- Workspace ID: 36ef6570-f84b-4d04-8131-7f403f4a3f04
- Workspace type: Personal

Fabric Trial capacity:

- SKU: FTL64
- Region: East US 2

Target Lakehouse:

- Name: lh_medallion_demo
- Lakehouse ID: 8429b9a8-1382-4024-b57d-f9afaeaddb25

Target pipeline:

- pl_medallion_github

Never use another Fabric workspace unless explicitly instructed.

Treat `config/dev.json` as the authoritative environment configuration.

Do not hard-code values in generated project code when they are already
available in `config/dev.json`.

Before performing any Fabric write operation, verify that the workspace
resolved from `config/dev.json` matches the intended Fabric workspace.

---

## MCP Strategy

### Fabric Local MCP

Server:

- fabric-local

Use primarily for:

- Fabric documentation
- item definitions
- API specifications
- API examples
- Fabric best practices
- schema discovery
- OneLake discovery/validation when appropriate

Before generating or modifying a Fabric definition, retrieve the current
Fabric definition or documentation when necessary rather than relying only
on model knowledge.

---

### Microsoft DataFactory MCP

Server:

- datafactory

Package:

- Microsoft.DataFactory.MCP
- version 0.24.0-beta

Use for live Microsoft Fabric Data Factory operations, including:

- authentication
- connection discovery/creation when supported
- pipeline discovery
- pipeline creation/update
- pipeline definition read/update
- pipeline execution
- pipeline run-status validation

Interactive authentication has been validated.

Do not assume a Fabric resource exists.

Discover or retrieve resources before modifying them.

Check for existing resources before creating new ones.

Reuse existing resources when they match the required configuration.

---

### Fabric Core MCP

Do not use fabric-core unless explicitly instructed.

It is not part of the required execution path for this lab.

The current Codex/Fabric Core OAuth integration has not been validated
successfully in this environment.

---

### OneLake Operations

Use Fabric Local OneLake tools only after verifying that authentication
and the required operation work in the current environment.

Start with read-only discovery.

Do not upload, overwrite, move, or delete OneLake content until the
target Lakehouse and path have been verified.

---

### REST API Fallback

Use official Microsoft Fabric REST APIs only when the required operation
cannot be performed with the validated MCP tools.

Before generating REST payloads:

1. Retrieve the current API specification through Fabric Local MCP.
2. Retrieve the current item definition when applicable.
3. Validate required fields.
4. Do not invent Fabric API schemas.

---

## Source Data

Repository:

https://github.com/normanmartinez2/databricks_claude

The lab uses one reusable HTTP connection for the GitHub source.

Reusable HTTP source connection:

- Name: github_raw_normanmartinez2
- Type: HttpServer
- Base URL: https://raw.githubusercontent.com/normanmartinez2
- Authentication: Anonymous

Codex must first check whether this connection already exists.

If it exists and matches the required configuration, reuse it.

If it does not exist, create it.

Use this connection for all GitHub CSV source files.

### airports.csv

Full URL:

https://raw.githubusercontent.com/normanmartinez2/databricks_claude/refs/heads/main/airports.csv

Connection-relative path:

databricks_claude/refs/heads/main/airports.csv

### bookings.csv

Full URL:

https://raw.githubusercontent.com/normanmartinez2/databricks_claude/refs/heads/main/bookings.csv

Connection-relative path:

databricks_claude/refs/heads/main/bookings.csv

### passengers.csv

Full URL:

https://raw.githubusercontent.com/normanmartinez2/databricks_claude/refs/heads/main/passengers.csv

Connection-relative path:

databricks_claude/refs/heads/main/passengers.csv

Before implementing ingestion:

1. Validate that all three source files are reachable.
2. Inspect the CSV headers and schemas.
3. Record source row counts.
4. Confirm the required join keys exist:
   - bookings.passenger_id
   - passengers.passenger_id
   - bookings.airport_id
   - airports.airport_id

Source URLs and environment-specific values should be obtained from
`config/dev.json` when they are defined there.

---

## Bronze Layer

### Purpose

Land the source datasets in OneLake while preserving the source data
with minimal transformation.

Bronze represents the first persistent layer of the Medallion architecture.

Flow:

GitHub CSV
    -> reusable HTTP connection
    -> Fabric Data Factory Copy Activity
    -> Lakehouse Files/bronze

### Source

Reusable HTTP connection:

- github_raw_normanmartinez2

Each source file must use:

- the reusable HTTP connection
- its corresponding file-specific relative path

Do not configure each Copy Activity with an unrelated or duplicate
HTTP connection.

### Destination

Target Lakehouse:

- lh_medallion_demo

Target format:

- CSV

Target paths:

- Files/bronze/airports/airports.csv
- Files/bronze/passengers/passengers.csv
- Files/bronze/bookings/bookings.csv

The destination must be configured as a valid Fabric Lakehouse sink.

The Lakehouse destination may be represented using the Fabric-generated
Lakehouse linked-service/reference containing the target workspace,
Lakehouse artifact and Files path.

A separate Lakehouse connection is not required if Fabric uses a valid
inline Lakehouse linked-service/reference in the pipeline definition.

Do not invent or force a separate Lakehouse connection when the supported
Fabric pipeline definition does not require one.

### Copy Activities

Bronze must use one Copy Activity per source file.

Activity names:

- Copy_airports_to_bronze
- Copy_passengers_to_bronze
- Copy_bookings_to_bronze

Each Bronze Copy Activity must explicitly configure both its source and
its destination.

Each Copy Activity must have:

Source:

- reusable HTTP connection: github_raw_normanmartinez2
- corresponding file-specific relative path

Destination:

- Lakehouse: lh_medallion_demo
- corresponding file-specific Bronze path under Files/bronze/

Required mappings:

Copy_airports_to_bronze:

- Source: airports.csv
- Destination: Files/bronze/airports/airports.csv

Copy_passengers_to_bronze:

- Source: passengers.csv
- Destination: Files/bronze/passengers/passengers.csv

Copy_bookings_to_bronze:

- Source: bookings.csv
- Destination: Files/bronze/bookings/bookings.csv

Use:

- Binary source
- Binary sink
- Binary -> Binary copy

Bronze must:

- preserve original source columns and values
- preserve the source CSV content
- perform no business transformations
- avoid unnecessary schema manipulation during ingestion

---

## Silver Layer

### Purpose

Clean, validate and enrich the Bronze datasets.

Input:

- Bronze CSV files

Target format:

- Parquet

Target path:

- Files/silver/bookings_enriched/

Required joins:

bookings.passenger_id = passengers.passenger_id

bookings.airport_id = airports.airport_id

Before implementing Silver:

1. Inspect the actual Bronze schemas.
2. Confirm the join columns exist.
3. Validate data types.
4. Check nulls.
5. Check duplicate keys.
6. Record Bronze row counts.

Do not assume referential integrity.

Silver must be validated before Gold processing starts.

---

## Gold Layer

### Purpose

Produce a business-ready analytical aggregate from the Silver dataset.

Input:

- Files/silver/bookings_enriched/

Target format:

- Parquet

Target path:

- Files/gold/bookings_by_city/

Required aggregation:

GROUP BY city

Metric:

- booking_count

Before implementing Gold, verify which field represents city after the
Silver join.

Do not infer the city column if the resulting schema is ambiguous.

Gold must only execute after Silver has completed successfully.

---

## Orchestration

Target Fabric Data Factory pipeline:

- pl_medallion_github

This is the main orchestration pipeline for the lab.

Do not create separate Bronze, Silver or Gold pipelines unless explicitly
required by the exercise.

The final pipeline orchestrates:

GitHub
    -> Bronze
    -> Silver
    -> Gold

The initial Bronze implementation contains:

- Copy_airports_to_bronze
- Copy_passengers_to_bronze
- Copy_bookings_to_bronze

The three Bronze Copy Activities are independent source ingestions and may
run independently unless a technical dependency requires otherwise.

Silver must not run until all required Bronze activities have succeeded.

Gold must not run until Silver has succeeded.

Use explicit activity dependencies for the Medallion layer transitions.

Apply retry policies only when appropriate and supported by the retrieved
Fabric definition.

---

## Development Workflow

For every significant Fabric change:

1. Read `AGENTS.md` and `config/dev.json`.
2. Discover the relevant current Fabric state.
3. Check for existing resources.
4. Reuse existing resources when they match the required configuration.
5. Retrieve current Fabric documentation/schema when necessary.
6. Validate workspace and target resource IDs.
7. Generate or update the required definition.
8. Execute the requested change using the appropriate MCP.
9. Read the resulting Fabric resource back when supported.
10. Compare the deployed definition with the intended definition.
11. Run validation when appropriate.
12. Report the result and all Fabric resources created or modified.

Do not consider a deployment successful only because a create or update
operation returned success.

Read the resulting Fabric resource back whenever the MCP supports it.

Codex may create local project artifacts required for:

- implementation
- generated Fabric definitions
- before/after comparisons
- debugging
- execution evidence
- validation

Do not unnecessarily restrict local development artifacts.

---

## Safety Rules

- DEV only.
- Never use PROD.
- Never use another workspace without explicit instruction.
- Never delete Fabric resources without explicit approval.
- Never create schedules unless explicitly requested.
- Check for existing resources before creating new ones.
- Avoid duplicate resources on repeated execution.
- Do not invent IDs, URLs, schemas, credentials or Fabric capabilities.
- Do not expose credentials or authentication tokens.
- Do not create unrelated Fabric resources.
- Prefer MCP-returned facts over assumptions.
- Report Fabric errors rather than hiding them.
- Do not silently switch to another workspace, Lakehouse, path or API.
- Validate the target workspace before Fabric write operations.

---

## Validation Requirements

### Bronze

Validate:

- all three source files are reachable
- all three source schemas can be inspected
- source row counts are recorded
- all three target Bronze files exist
- expected source columns are preserved
- target row counts are recorded
- source and target row counts match
- Copy Activities use the intended reusable HTTP source connection
- Copy Activities use the intended source relative paths
- Copy Activities target the intended Lakehouse
- Copy Activities target the intended Bronze paths
- deployed pipeline definition matches the intended Bronze definition
- Bronze pipeline execution completes successfully

### Silver

Validate:

- Parquet output exists
- join keys are valid
- output schema is recorded
- row counts are recorded
- null behavior is understood
- duplicate behavior is understood
- Silver execution completes successfully

### Gold

Validate:

- Parquet output exists
- city is present
- booking_count is present
- aggregation results are non-empty when Silver contains data
- Gold execution completes successfully

### Pipeline

Validate:

- deployed definition matches the intended definition
- activity dependencies match the Medallion flow
- pipeline executes successfully
- final run status is Succeeded
- failures are reported rather than hidden

---

## Agent Behaviour

Codex / GPT-6 Astra must distinguish between:

- facts retrieved through MCP
- information read from project files
- assumptions
- generated implementation proposals

When current Fabric documentation can be retrieved through MCP, prefer
retrieval over relying solely on model knowledge.

If an MCP tool returns an error:

1. Report and diagnose the error.
2. Check the relevant Fabric definition or documentation.
3. Correct the implementation when possible.
4. Only use another mechanism when necessary.

Do not silently fall back to another workspace, Fabric resource, storage
path or API.

Keep final responses concise unless detailed diagnostic information is
required.