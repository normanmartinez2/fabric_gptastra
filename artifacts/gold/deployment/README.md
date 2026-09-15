# Gold deployment validation

- Created nb_gold_bookings_by_city (a13d249b-0485-41d7-ab6f-e758144f0e26) with approved notebook code and the existing lh_medallion_demo default Lakehouse. Definition readback matched approved code and binding.
- Updated pl_medallion_github with Notebook_Silver_to_Gold, depending exclusively on Notebook_Bronze_to_Silver Succeeded. All four prior activities remained identical. No connection created.
- Pipeline run 55cca996-654a-4452-8f16-ff10f95ca5d3 completed without failure at 2026-09-15T06:07:29.46Z. Gold Spark run ad88628e-7d20-47d4-a869-aa36a82626c0 Succeeded.
- Downloaded all 39 Gold Parquet parts and the current Silver part through Fabric Local OneLake. Independent PyArrow validation passed: 1,000 input and eligible rows, zero excluded, 50 Gold rows, sum 1,000. Every city aggregate matches Silver. Schema exactly city STRING and booking_count BIGINT.
- Silver logical content matches the approved design snapshot. Gold writes only Files/gold/bookings_by_city/. The requested full pipeline reran its existing upstream activities.
- Evidence: notebook-verification.json, pipeline.before/intended/deployed.json, pipeline.run-status.json, notebook.sessions.json, gold-validation.json and downloaded Parquet files.
- Data Factory status lookup initially encountered an expired token; interactive reauthentication resolved it. No remaining blockers.
