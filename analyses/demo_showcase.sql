-- Demo queries: Customer Profitability & Margin (Gold layer)
-- ------------------------------------------------------------------------------
-- Prerequisites: dbt has been built (dbt snapshot; dbt build). Adjust DATABASE
-- and SCHEMA if your target differs from PROPELLINGTECH_TPCH / GOLD_PROFITABILITY.
-- Run sections in a Snowflake worksheet (semicolon-separated statements allowed).
-- ------------------------------------------------------------------------------

USE DATABASE PROPELLINGTECH_TPCH;
USE SCHEMA GOLD_PROFITABILITY;
-- USE WAREHOUSE PROPELLINGTECH_WH;  -- optional if your worksheet already has a warehouse

-- ---------------------------------------------------------------------------
-- 1) Headline portfolio metrics (line grain → rolled up)
--    Showcases: fct additive measures, one source of truth for $ totals
-- ---------------------------------------------------------------------------
SELECT
  COUNT(*) AS lineitem_rows,
  COUNT(DISTINCT order_id) AS distinct_orders,
  SUM(net_revenue) AS total_net_revenue,
  SUM(gross_margin) AS total_gross_margin,
  SUM(discount_amount) AS total_discount,
  CASE WHEN SUM(net_revenue) > 0
    THEN SUM(gross_margin) / NULLIF(SUM(net_revenue), 0) END AS portfolio_margin_rate
FROM FCT_SALES_LINEITEM;

-- ---------------------------------------------------------------------------
-- 2) “Canned” customer profitability — last 90 order days in the fact
--    (anchor = max(order_date) in the fact, not wall-clock; see model comments)
--    Showcases: rpt_customer_profitability_90d, tier + segment, discount vs margin
-- ---------------------------------------------------------------------------
SELECT
  customer_id,
  customer_name,
  market_segment,
  customer_tier,
  region_name,
  is_lapsed,
  order_count,
  lineitem_count,
  total_net_revenue,
  total_gross_margin,
  margin_rate,
  discount_rate_effective
FROM RPT_CUSTOMER_PROFITABILITY_90D
ORDER BY total_gross_margin DESC
LIMIT 25;

-- ---------------------------------------------------------------------------
-- 3) “Canned” segment concentration — by calendar month
--    Showcases: margin_concentration_index (>1 = margin over-index vs revenue)
-- ---------------------------------------------------------------------------
SELECT
  order_year_month_key,
  market_segment,
  total_net_revenue,
  total_gross_margin,
  margin_rate,
  revenue_share,
  margin_share,
  margin_concentration_index
FROM RPT_SEGMENT_MARGIN_CONCENTRATION
WHERE order_year_month_key = (
  SELECT MAX(order_year_month_key) FROM RPT_SEGMENT_MARGIN_CONCENTRATION
)
ORDER BY total_net_revenue DESC;

-- ---------------------------------------------------------------------------
-- 4) All-time top customers by gross margin (natural key = customer_id)
--    Showcases: fact + SCD2 dim — join on customer_key; group by customer_id
-- ---------------------------------------------------------------------------
SELECT
  dc.customer_id,
  MAX(dc.customer_name) AS customer_name,
  MAX(dc.market_segment) AS market_segment,
  MAX(dc.customer_tier) AS customer_tier,
  MAX(dc.region_name) AS region_name,
  SUM(f.net_revenue) AS total_net_revenue,
  SUM(f.gross_margin) AS total_gross_margin,
  CASE WHEN SUM(f.net_revenue) > 0
    THEN SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0) END AS margin_rate
FROM FCT_SALES_LINEITEM f
INNER JOIN DIM_CUSTOMER dc
  ON f.customer_key = dc.customer_key
GROUP BY dc.customer_id
ORDER BY total_gross_margin DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 5) Revenue rank vs margin rank — do “big revenue” and “big margin” align?
--    (CTE ranks on all-time customer totals)
--    Showcases: Commercial Finance question from ADR / README
-- ---------------------------------------------------------------------------
WITH by_customer AS (
  SELECT
    dc.customer_id,
    SUM(f.net_revenue) AS total_net_revenue,
    SUM(f.gross_margin) AS total_gross_margin
  FROM FCT_SALES_LINEITEM f
  INNER JOIN DIM_CUSTOMER dc ON f.customer_key = dc.customer_key
  GROUP BY dc.customer_id
),
ranked AS (
  SELECT
    customer_id,
    total_net_revenue,
    total_gross_margin,
    RANK() OVER (ORDER BY total_net_revenue DESC) AS revenue_rank,
    RANK() OVER (ORDER BY total_gross_margin DESC) AS margin_rank
  FROM by_customer
)
SELECT
  customer_id,
  total_net_revenue,
  total_gross_margin,
  revenue_rank,
  margin_rank,
  (margin_rank - revenue_rank) AS rank_gap
FROM ranked
WHERE ABS(margin_rank - revenue_rank) > 10
ORDER BY ABS(margin_rank - revenue_rank) DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 6) Bill-side geography (customer nation) — margin by region
--    Showcases: conformed dim_geography on BILL path
-- ---------------------------------------------------------------------------
SELECT
  g.region_name,
  g.nation_name,
  SUM(f.net_revenue) AS total_net_revenue,
  SUM(f.gross_margin) AS total_gross_margin
FROM FCT_SALES_LINEITEM f
INNER JOIN DIM_GEOGRAPHY g ON f.bill_geography_key = g.geography_key
GROUP BY g.region_name, g.nation_name
ORDER BY total_gross_margin DESC;

-- ---------------------------------------------------------------------------
-- 7) Order priority — margin and discount load by TPC-H priority
--    Showcases: degenerate order attributes on the fact
-- ---------------------------------------------------------------------------
SELECT
  f.order_priority,
  COUNT(*) AS lines,
  SUM(f.net_revenue) AS total_net_revenue,
  SUM(f.gross_margin) AS total_gross_margin,
  CASE WHEN SUM(f.net_revenue) > 0
    THEN SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0) END AS margin_rate,
  CASE WHEN SUM(f.extended_price) > 0
    THEN SUM(f.discount_amount) / NULLIF(SUM(f.extended_price), 0) END AS effective_discount_rate
FROM FCT_SALES_LINEITEM f
GROUP BY f.order_priority
ORDER BY f.order_priority;

-- ---------------------------------------------------------------------------
-- 8) “Margin erosion” — high discount, weak margin (line-level)
--    Showcases: line grain preserved in Gold (ADR-03)
-- ---------------------------------------------------------------------------
SELECT
  f.order_id,
  f.line_number,
  f.order_date,
  d.part_type,
  f.discount_rate,
  f.net_revenue,
  f.gross_margin,
  f.margin_rate
FROM FCT_SALES_LINEITEM f
INNER JOIN DIM_PART d ON f.part_key = d.part_key
WHERE f.discount_rate >= 0.07
  AND f.margin_rate IS NOT NULL
  AND f.margin_rate < 0.20
ORDER BY f.net_revenue DESC
LIMIT 30;

-- ---------------------------------------------------------------------------
-- 9) Time trend — month rollups (dim_date on order key)
--    Showcases: order_year_month_key on fact, optional calendar labels from dim_date
-- ---------------------------------------------------------------------------
SELECT
  dd.year_number,
  dd.month_number,
  dd.month_long_name,
  SUM(f.net_revenue) AS total_net_revenue,
  SUM(f.gross_margin) AS total_gross_margin
FROM FCT_SALES_LINEITEM f
INNER JOIN DIM_DATE dd ON f.order_date_key = dd.date_key
GROUP BY dd.year_number, dd.month_number, dd.month_long_name
ORDER BY dd.year_number, dd.month_number;

-- ---------------------------------------------------------------------------
-- 10) Product mix — part type and brand margin contribution
--     Showcases: dim_part, contribution-style analysis
-- ---------------------------------------------------------------------------
SELECT
  p.part_type,
  p.brand,
  SUM(f.net_revenue) AS total_net_revenue,
  SUM(f.gross_margin) AS total_gross_margin
FROM FCT_SALES_LINEITEM f
INNER JOIN DIM_PART p ON f.part_key = p.part_key
GROUP BY p.part_type, p.brand
HAVING SUM(f.net_revenue) > 0
ORDER BY total_gross_margin DESC
LIMIT 30;

-- ---------------------------------------------------------------------------
-- 11) Customer tier lens — as captured on the fact join (as-of order)
--     (tier lives on dim_customer; fact holds SCD2 key for order date)
--     Showcases: Strategic / Growth / Maintain / Watch
-- ---------------------------------------------------------------------------
SELECT
  dc.customer_tier,
  COUNT(*) AS lineitem_rows,
  SUM(f.net_revenue) AS total_net_revenue,
  SUM(f.gross_margin) AS total_gross_margin
FROM FCT_SALES_LINEITEM f
INNER JOIN DIM_CUSTOMER dc ON f.customer_key = dc.customer_key
GROUP BY dc.customer_tier
ORDER BY total_gross_margin DESC;

-- ---------------------------------------------------------------------------
-- 12) SCD2 footprint (optional) — how many version rows per customer
--     Showcases: snapshot-driven history (static TPC-H: often one row)
-- ---------------------------------------------------------------------------
SELECT
  customer_id,
  COUNT(*) AS scd2_versions
FROM DIM_CUSTOMER
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY scd2_versions DESC, customer_id
LIMIT 20;
