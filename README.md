# propellingtech_tpch

This repo implements a **Customer Profitability & Margin Analysis** data product on **Snowflake** and **dbt**. **Before reading any code, read [ADR.md](./ADR.md)** — it documents the **twelve** architectural decisions (ADR-01 through ADR-12) that shape every model. The code is the artifact; the reasoning is the deliverable.

**Stack:** Snowflake, dbt (Snowflake adapter), TPC-H sample data `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`.

**Dataset:** TPC-H SF1 (`SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`). A Snowflake database you control (e.g. `PROPELLINGTECH_TPCH`) holds built schemas and objects.

---

## Design intent

The analytical layer is aimed at **Commercial Finance**: which customers to invest in, retain, or deprioritize, and how margin and revenue behave by segment, geography, and order attributes. Full rationale for Bronze / Silver / Gold, grain, SCD, testing, and materialization is in **[ADR.md](./ADR.md)**.

---

## How this was built

This project was built using **AI-augmented delivery**. All architectural decisions — documented in [ADR.md](./ADR.md) — were made by the author. An LLM translated those specifications into dbt SQL; every model was validated against the ADR, covered by **161** dbt tests (all passing on the author’s Snowflake target), and reviewed for consistency with the documented intent. That mirrors how the author would run a client engagement: human judgment at the architecture and validation layers, AI leverage at the execution layer.

---

## Scope delivered


| Layer / artifact                                                                          | Status   |
| ----------------------------------------------------------------------------------------- | -------- |
| [ADR.md](./ADR.md) — reasoning                                                            | Complete |
| Project scaffolding (`dbt_project.yml`, packages, macros)                                 | Complete |
| `generate_schema_name` override (ideally would be handled by CI/CD / Github Actions)      | Complete |
| `add_ingestion_metadata` macro                                                            | Complete |
| Bronze (8 models + sources YAML + schema YAML)                                            | Complete |
| `snap_customer_scd2` (SCD Type 2 snapshot)                                                | Complete |
| Silver (8 models + schema YAML + tests)                                                   | Complete |
| Custom generic test `positive_value`                                                      | Complete |
| Gold dimensions (`dim_customer`, `dim_supplier`, `dim_part`, `dim_geography`, `dim_date`) | Complete |
| Gold fact `fct_sales_lineitem`                                                            | Complete |
| Report views (`rpt_customer_profitability_90d`, `rpt_segment_margin_concentration`)       | Complete |
| Singular test `margin_reconciliation`                                                     | Complete |


Further hardening (CI, exposures, model contracts, source freshness) can be added as the engagement matures.

---

## Repository layout

```
propellingtech_tpch/
├── ADR.md
├── README.md
├── dbt_project.yml
├── packages.yml
├── .gitignore
├── macros/
│   ├── generate_schema_name.sql       # Honors +schema verbatim (no dev prefix)
│   └── add_ingestion_metadata.sql     # Bronze metadata columns
├── models/
│   ├── bronze/                        # → bronze_tpch (ADR-11)
│   │   ├── _bronze__sources.yml
│   │   ├── _bronze__models.yml
│   │   ├── tpch_customer.sql
│   │   ├── tpch_orders.sql
│   │   ├── tpch_lineitem.sql
│   │   ├── tpch_part.sql
│   │   ├── tpch_supplier.sql
│   │   ├── tpch_partsupp.sql
│   │   ├── tpch_nation.sql
│   │   └── tpch_region.sql
│   ├── silver/                        # domain schemas (ADR-11)
│   │   ├── _silver__models.yml
│   │   ├── customer/
│   │   │   └── customer.sql         # From SCD2 snapshot
│   │   ├── sales/
│   │   │   ├── orders.sql
│   │   │   └── lineitem.sql         # net_revenue here (ADR-02)
│   │   ├── purchasing/
│   │   │   ├── part.sql
│   │   │   ├── supplier.sql
│   │   │   └── partsupp.sql
│   │   └── geography/
│   │       ├── nation.sql
│   │       └── region.sql
│   └── gold/                          # → gold_profitability (ADR-11)
│       ├── _gold__models.yml
│       ├── dim_date.sql               # dbt_utils.date_spine
│       ├── dim_customer.sql
│       ├── dim_supplier.sql
│       ├── dim_part.sql
│       ├── dim_geography.sql
│       ├── fct_sales_lineitem.sql    # gross_margin here (ADR-02)
│       ├── rpt_customer_profitability_90d.sql
│       └── rpt_segment_margin_concentration.sql
├── snapshots/
│   └── snap_customer_scd2.sql         # → silver_customer
├── tests/
│   ├── generic/
│   │   └── test_positive_value.sql
│   └── singular/
│       └── test_margin_reconciliation.sql
└── seeds/                             # optional CSV seeds (none in repo today)
```

---

## Setup

### Prerequisites

- Python 3.11+
- Snowflake account with usage on `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1` (trial accounts typically work)
- A database you own for project objects

### 1. Install dbt (project virtualenv)

```bash
python -m venv .venv
source .venv/bin/activate                    # Linux / macOS
# Windows (PowerShell):  .\.venv\Scripts\Activate.ps1
pip install dbt-snowflake
```

From the project root, dbt is available as `.venv/Scripts/dbt.exe` (Windows) or `.venv/bin/dbt` (Unix).

### 2. Configure `profiles.yml`

Create or edit `~/.dbt/profiles.yml` (Windows: `%USERPROFILE%\.dbt\profiles.yml`):

```yaml
propellingtech_tpch:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account_identifier>
      user: <your_user>
      password: <your_password>          # or key-pair / SSO
      role: <your_role>
      database: PROPELLINGTECH_TPCH
      warehouse: <your_warehouse>
      schema: gold                       # fallback; layer configs override
      threads: 4
      client_session_keep_alive: false
```

### 3. Create schemas in Snowflake

Segmentation follows **ADR-11** (source / domain / product). Example:

```sql
create database if not exists propellingtech_tpch;

create schema if not exists propellingtech_tpch.bronze_tpch;
create schema if not exists propellingtech_tpch.silver_customer;
create schema if not exists propellingtech_tpch.silver_sales;
create schema if not exists propellingtech_tpch.silver_purchasing;
create schema if not exists propellingtech_tpch.silver_geography;
create schema if not exists propellingtech_tpch.gold_profitability;
```

### 4. Connection and packages

```bash
dbt debug
dbt deps
```

### 5. Build

```bash
dbt snapshot                    # SCD2 customer snapshot
dbt build                       # models + tests

# Optional: by layer
dbt build --select tag:bronze
dbt build --select tag:silver
dbt build --select tag:gold
```

### 6. Documentation

```bash
dbt docs generate
dbt docs serve
```

Lineage in the docs site is the fastest way to see the end-to-end DAG.

---

## Architecture quick reference

Details are in [ADR.md](./ADR.md).


| ADR        | Decision (summary)                                                                                                                                 |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ADR-01** | Medallion: source, business logic, and consumption change at different rates.                                                                      |
| **ADR-02** | Silver vs Gold: reusable conformation in Silver; product-specific metrics (e.g. `gross_margin`) in Gold. `net_revenue` in Silver for this dataset. |
| **ADR-03** | Fact grain: one row per order line; do not aggregate away line detail upstream.                                                                    |
| **ADR-04** | SCD Type 2 on `dim_customer` via snapshots; Type 1 on supplier, part, geography.                                                                   |
| **ADR-05** | Bronze as tables with ingestion metadata (not pass-through views only).                                                                            |
| **ADR-06** | All eight TPC-H tables promoted where this product needs them, including `partsupp` for supply cost.                                               |
| **ADR-07** | Source-shaped names in Bronze; business names in Silver; `dim_` / `fct_` / `rpt_` in Gold for role clarity.                                        |
| **ADR-08** | Progressive testing; custom `positive_value`; singular `margin_reconciliation`.                                                                    |
| **ADR-09** | Tables for Bronze/Silver/Gold core; `rpt_`* as views at this scale; incremental fact at higher volume.                                             |
| **ADR-10** | Out of scope: separate supplier analytics product, real-time ingest for this benchmark, RLS demo, semantic layer v1, returns (not in TPC-H).       |
| **ADR-11** | Schemas: `bronze_<source>`, `silver_<domain>`, `gold_<product>`.                                                                                   |
| **ADR-12** | ID standardization / quarantine pattern documented; deferred while TPC-H is the only source.                                                       |


---

**Author:** Joseph Oghali · April 2026