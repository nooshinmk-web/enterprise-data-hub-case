# Enterprise Data Hub - Technical Case

A reusable data product integrating vessel and weather data
for maritime analytics.

## Architecture

docs/architecture.png

The solution follows a Medallion Architecture:

- **Bronze:** Raw API responses preserved for traceability and replay.
- **Silver:** Cleaned, standardized, validated data.
- **Gold:** Analytical fact and dimension tables for consumption.

## Data Sources

- **VesselAPI** — vessel master and operational data.
- **MET Norway Weather API** — weather forecast data.

API ingestion is metadata-driven using `config/sources.json`.

The generic REST ingestion pipeline reads source configuration dynamically,
including:

- API base URL
- Relative URL
- Authentication type
- Secret name
- Target folder
- HTTP method

This allows additional API sources to be onboarded with minimal pipeline changes.

## Event-Driven Processing

ADF writes raw API responses to the Bronze layer in ADLS.

When a new weather JSON file is created under:

`bronze/weather/`

a Storage Event Trigger fires through Azure Event Grid.

The trigger starts `pl_transform_bronze_to_gold`, which retrieves the
Databricks access token from Azure Key Vault and triggers a Databricks Job.

The Databricks Job executes:

`bronze_transform → silver_transform → gold_transform`

This provides an event-driven transition from ingestion to analytical processing.

For this technical case, arrival of the weather file is used as the
completion signal for the ingestion cycle.

In production, this would be replaced by an explicit batch-completion event
or orchestration mechanism.

## Analytical Data Model

### Dimensions

- `dim_vessel` — vessel descriptive attributes with SCD Type 2 history.
- `dim_location` — weather forecast locations.
- `dim_date` — shared calendar dimension.

### Facts

- `fact_vessel_snapshot` — vessel operational snapshots over time.
- `fact_weather_forecast` — weather forecasts by location and timestamp.

The two fact tables are intentionally not directly joined because the
available datasets do not provide a reliable vessel-to-weather
spatiotemporal relationship.

## Historical Data

Two forms of history are supported:

- Vessel operational snapshots preserve metrics such as speed,
  draught and operating status over time.
- SCD Type 2 on `dim_vessel` preserves historical changes to vessel attributes.

## Data Quality

Checks are applied before publishing analytical data:

- Required business-key validation
- Duplicate-key detection
- Referential-integrity validation
- Deterministic deduplication
- Record-hash based change detection
- Quarantine handling
- Pipeline audit records

## Data Contract

The Gold data product is governed by:

`contracts/maritime-data-product.yaml`

The contract defines:

- Data product purpose
- Ownership
- Semantic versioning
- Change management

A target processing latency may be defined for production use, but
SLA monitoring and enforcement are outside the scope of this prototype.

## Security

Secrets are stored in Azure Key Vault.

Examples include:

- VesselAPI bearer token
- Databricks access token

ADF retrieves secrets at runtime rather than storing credentials directly
in pipeline configuration.

## Engineering Practices

The solution demonstrates:

- Metadata-driven ingestion
- Generic REST integration
- Parameterized pipelines
- Azure Key Vault secret management
- Event-driven processing with Event Grid
- Incremental Delta processing
- Idempotent MERGE operations
- Deterministic deduplication
- SCD Type 2
- Data-quality validation
- Audit logging
- Delta Lake storage
- Partitioning
- OPTIMIZE and Z-ORDER
- Serverless Databricks execution

## Repository Structure

enterprise-data-hub-case/
├── README.md
│
├── config/
│   └── sources.json
│
├── contracts/
│   └── maritime-data-product.yaml
│
├── databricks_notebooks/
│   ├── 1_Bronze_Ingestion.py
│   ├── 2_Silver_Transformation.py
│   └── 3_Gold_Analysis.py
│
├── docs/
│   └── architecture.png
│
└── sqlpool_queries/
    ├── dim_date.sql
    ├── dim_location.sql
    ├── dim_vessel.sql
    ├── fact_vessel_snapshot.sql
    ├── fact_weather_forecast.sql
    ├── pipeline_runs.sql
    ├── quarantine.sql
    ├── vessel.sql
    └── weather.sql

## Production Extensions

In a production environment, I would add:

- Git-integrated ADF development
- Dev/test/prod environment separation
- Unity Catalog governance and lineage
- Automated Power BI deployment and refresh

## Assumptions and Trade-offs

The solution was intentionally scoped to the technical-case timebox.

The weather dataset represents forecast data for configured locations and
is not directly associated with vessel position.

The weather file is used as the ingestion-completion signal for the
event-driven flow. This is suitable for the prototype but would be replaced
by explicit batch orchestration in production.