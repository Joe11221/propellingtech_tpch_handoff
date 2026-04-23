-- fct_sales_lineitem (gold)
-- Line-level fact: (order_id, line_number) grain (ADR-03). Dims: SCD2 customer
-- (as-of order_date), SCD1 part/supplier, geography for ship/bill, date keys.
-- gross_margin, supply_cost, margin_rate computed here; net_revenue from Silver (ADR-02).
-- At much larger scale: see incremental/partition note in ADR-09.

{{
    config(
        materialized = 'table',
        tags         = ['gold', 'fact', 'tier_1']
    )
}}

-- ---------------------------------------------------------------------------
-- Base: lineitem + order (to get customer_id and order_date for SCD2 lookup)
-- ---------------------------------------------------------------------------
with lineitem as (

    select
        li.order_id,
        li.line_number,
        li.part_id,
        li.supplier_id,
        li.quantity,
        li.extended_price,
        li.discount_rate,
        li.discount_amount,
        li.tax_rate,
        li.tax_amount,
        li.net_revenue,
        li.return_flag,
        li.line_status,
        li.ship_date,
        li.commit_date,
        li.receipt_date,
        li.ship_mode,
        li.ship_instruction
    from {{ ref('lineitem') }} as li

),

orders as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status_code,
        o.order_priority,
        o.ship_priority
    from {{ ref('orders') }} as o

),

-- ---------------------------------------------------------------------------
-- SCD2-aware customer resolution
--
-- Join a lineitem's order_date between the customer version's valid_from
-- and valid_to. In TPC-H the snapshot will typically only have one version
-- per customer, so this resolves cleanly — but the join pattern is the
-- correct one for real SCD2 data and is what we'd demo live.
-- ---------------------------------------------------------------------------
scd_resolved as (

    select
        li.*,
        o.customer_id,
        o.order_date,
        o.order_status_code,
        o.order_priority,
        o.ship_priority,
        dc.customer_key,
        dc.nation_id           as bill_nation_id
    from lineitem               as li
    inner join orders           as o  on li.order_id = o.order_id
    left join {{ ref('dim_customer') }} as dc
           on o.customer_id = dc.customer_id
          and cast(o.order_date as timestamp) >= dc.valid_from
          and cast(o.order_date as timestamp) <  dc.valid_to

),

-- ---------------------------------------------------------------------------
-- Supply cost: join (part_id, supplier_id) → supply_cost_per_unit
-- ---------------------------------------------------------------------------
with_supply_cost as (

    select
        sr.*,
        ps.supply_cost_per_unit
    from scd_resolved          as sr
    left join {{ ref('partsupp') }} as ps
           on sr.part_id     = ps.part_id
          and sr.supplier_id = ps.supplier_id

),


-- Geography resolution — ship (supplier nation) and bill (customer nation)

with_geography as (

    select
        ws.*,
        ds.supplier_key,
        ds.nation_id                as ship_nation_id,
        ship_geo.geography_key       as ship_geography_key,
        bill_geo.geography_key       as bill_geography_key,
        dp.part_key
    from with_supply_cost              as ws
    left join {{ ref('dim_supplier') }}   as ds  on ws.supplier_id = ds.supplier_id
    left join {{ ref('dim_part') }}       as dp  on ws.part_id     = dp.part_id
    left join {{ ref('dim_geography') }}  as ship_geo  on ds.nation_id       = ship_geo.nation_id
    left join {{ ref('dim_geography') }}  as bill_geo  on ws.bill_nation_id  = bill_geo.nation_id

),

-- ---------------------------------------------------------------------------
-- Final projection with Gold-computed measures
-- ---------------------------------------------------------------------------
final as (

    select
        -- Primary surrogate — deterministic, one per lineitem grain row.
        {{ dbt_utils.generate_surrogate_key(['order_id', 'line_number']) }}
                                                            as sales_lineitem_key,

        -- Dimension foreign keys
        customer_key,
        supplier_key,
        part_key,
        ship_geography_key,
        bill_geography_key,

        -- Date keys (YYYYMMDD integer form — matches dim_date.date_key)
        cast(to_char(order_date,   'YYYYMMDD') as int)      as order_date_key,
        cast(to_char(ship_date,    'YYYYMMDD') as int)      as ship_date_key,
        cast(to_char(commit_date,  'YYYYMMDD') as int)      as commit_date_key,
        cast(to_char(receipt_date, 'YYYYMMDD') as int)      as receipt_date_key,

        -- Degenerate dimensions (order-level attributes with no need for their
        -- own dim — stored inline per Kimball's degenerate-dim pattern)
        order_id,
        line_number,
        order_status_code,
        order_priority,
        ship_priority,
        return_flag,
        line_status,
        ship_mode,
        ship_instruction,

        -- Raw dates (convenient for BI tools that prefer dates over surrogate keys)
        order_date,
        ship_date,
        commit_date,
        receipt_date,

        -- Additive measures
        quantity,
        extended_price,
        discount_amount,
        tax_amount,
        net_revenue,

        -- Gold-computed supply cost and margin
        (supply_cost_per_unit * quantity)::number(14,2)     as supply_cost,
        (net_revenue - (supply_cost_per_unit * quantity))::number(14,2)
                                                             as gross_margin,

        -- Non-additive rates (use weighted averages in BI, never simple avg)
        discount_rate,
        tax_rate,
        case
            when net_revenue > 0
            then ((net_revenue - (supply_cost_per_unit * quantity)) / net_revenue)::number(7,6)
        end                                                  as margin_rate

    from with_geography

)

select * from final
