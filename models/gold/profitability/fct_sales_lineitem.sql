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

-- Base: lineitem + order (customer_id, order_date for SCD2).
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

-- SCD2: customer_key where order_timestamp ∈ [valid_from, valid_to). SF1 usually
-- has one version per customer; the join pattern is the real-world one.
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

-- Supply cost from partsupp on (part_id, supplier_id).
with_supply_cost as (

    select
        sr.*,
        ps.supply_cost_per_unit
    from scd_resolved          as sr
    left join {{ ref('partsupp') }} as ps
           on sr.part_id     = ps.part_id
          and sr.supplier_id = ps.supplier_id

),

-- Ship geography = supplier’s nation; bill = customer’s nation → dim_geography.
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

-- Final: surrogate key, additive $, rates, margin.
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'line_number']) }}
                                                            as sales_lineitem_key,

        customer_key,
        supplier_key,
        part_key,
        ship_geography_key,
        bill_geography_key,

        cast(to_char(order_date,   'YYYYMM') as int)         as order_year_month_key,
        cast(to_char(order_date,   'YYYYMMDD') as int)      as order_date_key,
        cast(to_char(ship_date,    'YYYYMMDD') as int)      as ship_date_key,
        cast(to_char(commit_date,  'YYYYMMDD') as int)      as commit_date_key,
        cast(to_char(receipt_date, 'YYYYMMDD') as int)      as receipt_date_key,

        -- Degenerate dims (order/line attrs — no separate dimension table).
        order_id,
        line_number,
        order_status_code,
        order_priority,
        ship_priority,
        return_flag,
        line_status,
        ship_mode,
        ship_instruction,

        order_date,
        ship_date,
        commit_date,
        receipt_date,

        quantity,
        extended_price,
        discount_amount,
        tax_amount,
        net_revenue,

        (supply_cost_per_unit * quantity)::number(14,2)     as supply_cost,
        (net_revenue - (supply_cost_per_unit * quantity))::number(14,2)
                                                             as gross_margin,

        discount_rate,
        tax_rate,
        case
            when net_revenue > 0
            then ((net_revenue - (supply_cost_per_unit * quantity)) / net_revenue)::number(7,6)
        end                                                  as margin_rate

    from with_geography

)

select * from final
