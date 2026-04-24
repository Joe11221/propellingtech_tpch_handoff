# Architectural Decision Record
## Customer Profitability & Margin Analysis

**Author:** Joseph Oghali  
**Date:** April 2026  
**Status:** Accepted

---

## Purpose

This document records the **rationale** for the data model and build choices in this repository. It is the controlling specification: if a decision changes, the ADR is updated first, then the code.

---

## Design thesis

> *The product is a Customer Profitability analytical layer for Commercial Finance. Every choice—what sits in Bronze, Silver, and Gold, the fact grain, and what is intentionally out of scope—traces back to one question: **how does Commercial Finance decide which customers to invest in, retain, or deprioritize?***

Anything that does not serve that question does not belong in scope.

---

## Primary consumer

The main consumer of the Gold layer is **Commercial Finance leadership** at a wholesale-style distributor: for example a VP of Commercial Finance preparing for a recurring commercial review with Sales. They need to answer, without filing tickets to data engineering:

- Who are the most profitable customers, and what drives that profit?
- Which customers show high revenue but weak margin, and what patterns explain it?
- How does profitability spread across segments, geographies, and order-priority bands?
- Do the top customers by revenue align with the top customers by margin?
- How do discounts relate to margin erosion?

Gold is structured so those questions can be answered in standard BI tools (Power BI, Tableau, and similar).

---

## ADR-01: Why medallion (vs. a single layer)?

**Decision:** Three layers—Bronze, Silver, Gold—with explicit promotion rules between them.

**Reasoning:** Source systems, business rules, and analytical consumption change at different speeds.

- **Bronze** tracks **source change**: new columns, deprecations, drift.
- **Silver** tracks **business rule change**: definitions of active customer, discount treatment, and so on.
- **Gold** tracks **question change**: KPIs, segments, and report shapes.

A single combined layer couples those lifecycles: a small source tweak or a new dashboard requirement can force wide refactors. Medallion **limits blast radius**. The goal is operational change management, not layering for its own sake.

**Precedent:** At Cencora, the same separation of concerns applied to high-volume product dimensions (400K+ SKUs) with SCD Type 1/2 under Unity Catalog on Azure Databricks. The substrate here is Snowflake + dbt; the layering principle is unchanged.

---

## ADR-02: Silver vs. Gold placement

**Decision:** Use this rule for where a transformation belongs:

> *Could another team build a different analytical product on top of my Silver layer without reworking these transformations?*

If yes → Silver. If no → Gold.

**Examples:**

| Transformation | Layer | Reasoning |
| :--- | :--- | :--- |
| Type casting, column renaming | Silver | Universal cleanup, reusable |
| Null handling, deduplication | Silver | Hygiene, reusable |
| Referential integrity | Silver | Conformance, reusable |
| SCD Type 2 history | Silver (snapshots) | Entity truth, reusable |
| `net_revenue = extended_price × (1 - discount)` | **Silver** | Common definition of revenue after discount; centralizing avoids divergence across products |
| `gross_margin = net_revenue - supply_cost` | **Gold** | Margin definitions vary (gross, contribution, operating). Fixing one definition in Silver would be premature without a signed finance standard |
| Customer tier (Strategic / Growth / Maintain / …) | Gold | Strategy-specific thresholds |
| Segment rollups for specific reports | Gold | Consumption-specific |

**Judgment on `net_revenue`:** It could sit in Gold. It sits in Silver here because TPC-H does not model returns—the main source of ambiguity in “net revenue.” With returns in source, the boundary would be revisited with Finance.

---

## ADR-03: Fact grain

**Decision:** `fct_sales_lineitem` is **one row per order line item**.

**Reasoning:** The line is the atomic commercial event. Coarser grains discard detail permanently. Questions such as margin on high-discount lines require line-level facts.

**Principle:** You can always aggregate up; you cannot safely allocate down. Build at the atomic grain and roll up as needed.

---

## ADR-04: SCD strategy

**Decision:**

| Entity | Strategy | Mechanism |
| :--- | :--- | :--- |
| `dim_customer` | **SCD Type 2** | dbt snapshot on Silver |
| `dim_supplier` | SCD Type 1 | Overwrite |
| `dim_part` | SCD Type 1 | Overwrite |
| `dim_geography` | SCD Type 1 | Overwrite |
| `dim_date` | Static | Generated spine |

**Customer as SCD2:** Segment and balance change slowly and **affect margin attribution**. Revenue in a quarter must attach to the customer attributes that applied in that period.

**TPC-H note:** The benchmark data is static, so history is not strictly required for correctness on this extract. SCD2 is still implemented because commercial customer data in production is rarely static; the design should not require a redesign the first time history matters.

**Other dimensions as SCD1:** For this use case, supplier, part, and geography history are lower priority. If a future product (e.g. supplier risk) needs history, Bronze retains source shapes and snapshots can be added without breaking the fact grain.

---

## ADR-05: Bronze ingestion

**Decision:** Bronze is materialized as **tables**, not views over source, with three metadata columns per row.

**Rejected alternative:** View-over-source is quick when sample data already lives in the warehouse. It weakens **reproducibility and audit**: if the source changes, a view does not preserve what was seen at load time.

**Metadata** (macro `add_ingestion_metadata`):

- `_ingested_at` — load timestamp  
- `_source_system` — dbt var (e.g. `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`)  
- `_batch_id` — dbt `invocation_id` for run-level traceability  

**Production:** Against a live source, CDC (e.g. streams/tasks or incremental models with a high-water mark) would sit outside or beside this pattern. For a static benchmark, tables plus metadata are enough to show the contract without operating a CDC stack.

---

## ADR-06: What to land in Bronze vs. promote to Silver

**Bronze:** All eight TPC-H tables: `customer`, `orders`, `lineitem`, `part`, `supplier`, `partsupp`, `nation`, `region`.

**Silver:** All eight are promoted for this product:

- `customer`, `orders`, `lineitem` — transaction chain  
- `part`, `supplier` — descriptive dimensions  
- `partsupp` — **required** for unit supply cost (`ps_supplycost × quantity`); lineitem does not carry supply cost  
- `nation`, `region` — geography hierarchy  

**Principle:** Bronze is **source-complete**; Silver is **demand-driven**. Here, demand touches every table in the source. In a larger corporate model, many Bronze tables would never be promoted until a use case needs them.

---

## ADR-07: Naming

**Decision:** **Schema** carries layer (and domain where applicable). **Prefixes** in Gold encode star-schema role.

| Layer | Objects | Columns |
| :--- | :--- | :--- |
| Bronze | `<source>_<entity>` (e.g. `tpch_customer`) | Source names preserved (`C_CUSTKEY`) |
| Silver | `<entity>` in a domain schema (`customer`, `lineitem`) | Business names (`customer_id`) |
| Gold dimension | `dim_<entity>` | Business-readable |
| Gold fact | `fct_<entity>` | Business-readable |
| Gold reports | `rpt_<use_case>` | Business-readable |

**Bronze** stays auditable against source. **Silver** avoids redundant `sv_`-style prefixes when the schema already states the layer. **Gold** uses `dim_` / `fct_` / `rpt_` so BI consumers can scan object lists without guessing role.

Column renaming starts at **Silver** so the boundary between “as landed” and “conformed” stays obvious.

See **ADR-11** for how schemas are segmented.

---

## ADR-08: Testing

**Decision:** **Progressive rigor**—tests tighten by layer.

| Layer | Focus |
| :--- | :--- |
| Bronze | Availability, basic constraints, lineage-friendly checks |
| Silver | Quality, referential integrity, rule validity |
| Gold | Analytic correctness, reconciliations |

**In this codebase:**

- Custom **generic** test `positive_value` for reusable non-negativity checks where appropriate.  
- **Singular** test `margin_reconciliation`: fact-level margin sums must align with the customer report logic within tolerance, to catch silent rollup or join bugs.

The same idea—stricter checks downstream—was central to migration validation at Cencora (replacing manual spot checks across 26B+ row tables during SQL Server → Databricks); here it is expressed in dbt tests.

---

## ADR-09: Materialization

**Decision:**

| Area | Choice |
| :--- | :--- |
| Bronze | `table` |
| Silver | `table` |
| Gold dimensions | `table` |
| Gold fact | `table` at SF1 scale; **incremental** documented for SF10+ |
| Gold `rpt_*` | `view` |

**Scale-up (fact):** At much higher volume, `fct_sales_lineitem` would move to **incremental** with a stable `unique_key`, `on_schema_change` policy, partitioning on `order_date`, and clustering on high-cardinality keys such as `customer_key`.

**Why `rpt_*` are views here:**

- Facts are already materialized; views add orchestration-free freshness for thin rollups.  
- On Snowflake, predicate pushdown still applies; a view is not a full scan by definition.  
- At SF1, rollups over the fact are lightweight on a modest warehouse.

**If views become the bottleneck:** Promote selected `rpt_*` to **Dynamic Tables** with a `TARGET_LAG` aligned to the business cadence. That gives table-like performance with declarative refresh. **Materialized views** are a poor fit for the join/window shape of these rollups, so they are not the default upgrade path.

---

## ADR-10: Explicitly out of scope

1. **Supplier performance as a separate star** — Different product (procurement / risk). Gold stays scoped to customer profitability.  
2. **Real-time ingestion** — Not required for static TPC-H; production pattern is noted under ADR-05.  
3. **Row access policies / dynamic masking** — Real governance work; not meaningfully demonstrated on public sample data alone.  
4. **Semantic layer (Cube, MetricFlow, etc.)** — Logical next step after a stable Gold model; not part of this delivery.  
5. **Returns** — Not in TPC-H; `net_revenue` is post-discount only. With returns in source, Finance would own whether adjustment belongs in Silver (universal) or Gold (product-specific).

---

## ADR-11: Schema segmentation

**Decision:** Multiple schemas per layer: by **source** in Bronze, by **domain** in Silver, by **analytical product** in Gold.

| Layer | Pattern | This project |
| :--- | :--- | :--- |
| Bronze | `bronze_<source>` | `bronze_tpch` |
| Silver | `silver_<domain>` | `silver_customer`, `silver_sales`, `silver_purchasing`, `silver_geography` |
| Gold | `gold_<product>` | `gold_profitability` |

**Why:** Snowflake permissions and ownership align naturally to **schema**. Row policies and masking operate *within* schemas; they do not replace schema boundaries for coarse entitlements.

**TPC-H note:** One Bronze source and one Gold product means one schema each at those layers today. The layout is intended to **scale** (additional `bronze_*`, additional `gold_*`) without renaming everything later.

**Silver layout:**

| Schema | Contents |
| :--- | :--- |
| `silver_customer` | `customer`, snapshot |
| `silver_sales` | `orders`, `lineitem` |
| `silver_purchasing` | `part`, `supplier`, `partsupp` |
| `silver_geography` | `nation`, `region` |

Cross-schema FKs are valid in Snowflake when the role can use both schemas; dbt `ref()` and tests still enforce intended relationships.

**With ADR-07:** Schema placement + naming rules together define the contract for new objects.

---

## ADR-12: ID standardization and quarantine (deferred)

**Decision:** Document the **pattern** for canonical string IDs and quarantine; **do not implement** it while TPC-H is the only source. Native `bigint` keys remain correct for a single consistent source.

**When it matters:**

1. **Multi-source keys** — Different systems emit different string formats for the same entity.  
2. **Fixed-width export contracts** — Partners or regulators require padded IDs.  
3. **Composite-key stability in specific BI tools** — Sometimes a single padded key reduces join fragility.

**Why defer on TPC-H:** Applying padding everywhere now would push joins to `varchar`, hurt pruning/clustering on large facts, force wide test rewrites, and add quarantine plumbing for data that is clean by construction.

**Convention when activated:**

- Macro `standardize_id(column, length)` — cast to `varchar`, trim, `LPAD` to fixed width; overflows, unexpected characters, or nulls where forbidden route to quarantine.

**Illustrative length table** (headroom beyond TPC-H SF1000-scale counts):

| Column | Length | Rationale |
| :--- | :--- | :--- |
| `region_id` | 2 | Small enum |
| `nation_id` | 2 | Small enum |
| `customer_id` | 10 | Aligns with high-volume entity convention |
| `supplier_id` | 10 | Same convention as customer |
| `part_id` | 10 | Same |
| `order_id` | 12 | Highest-volume key; extra pad |
| `line_number` | 2 | Bounded within order |
| `clerk_id` | 10 | After stripping TPC-H `Clerk#` prefix, pad |

- Invalid rows → `<entity>__quarantine` in `silver_quarantine`, with `_quarantine_reason` (e.g. `id_overflow`, `id_non_digit`, `id_null`), `_quarantined_at`, `_batch_id` (invocation id), and `_raw_row` (e.g. VARIANT) for audit.

- Tests: `dbt_utils.expression_is_true` on `length(<id>) = N` where standardized; optional **warn**-severity row-count tests on quarantine tables so CI surfaces issues without always failing the build.

**Activation triggers:** Second source in Bronze, a signed export contract requiring fixed-width IDs, or a documented BI constraint that padding solves.

**Links:** **ADR-05** (raw keys preserved in Bronze enables Silver-only retrofit). **ADR-07** (Silver is already the “business-shaped” boundary). **ADR-10** (deferral is deliberate scope control).

---

## Operating principles

1. **Consumer first** — Design from the Commercial Finance questions backward.  
2. **Every object has a documented why** — No orphan models.  
3. **Trade-offs and non-goals are explicit** — Scope is a feature.  
4. **Reasoning and code stay aligned** — If they diverge, fix the ADR first.

---

When this document and the repository disagree, **update the ADR**, then align the code.
