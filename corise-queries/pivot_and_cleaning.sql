with automobile_customers_clean as (
  select 
      distinct 
          replace(c_custkey, ',', '') as customer_key
    from snowflake_sample_data.tpch_sf1.customer
    where 
      trim(lower(c_mktsegment)) = 'automobile'  
),


urgent_orders_clean as (
  select
    replace(o_custkey, ',', '') as order_customer_key,
    replace(o_orderkey, ',', '') as order_key,
    o_orderdate as order_date,
    ltrim(lower(o_orderpriority), '1-') as order_priority,
    replace(o_totalprice, ',', '') as total_price  
    from snowflake_sample_data.tpch_sf1.orders
    where 
      trim(lower(o_orderpriority)) = '1-urgent'
),


urgent_auto_customers as (
  select 
    automobile_customers_clean.customer_key,
    urgent_orders_clean.order_key,
    urgent_orders_clean.order_date,
    urgent_orders_clean.total_price 
  from urgent_orders_clean
  inner join automobile_customers_clean
    on urgent_orders_clean.order_customer_key = automobile_customers_clean.customer_key 
),


max_3_orders as (
  select 
    customer_key,
    order_key,
    order_date,
    total_price,
    row_number() over(partition by customer_key order by total_price asc) as row_num 
  from urgent_auto_customers
  qualify row_num <= 3
),


combine_max_3_orders as (
  select 
    customer_key,
    sum(total_price) as total_spent,
    listagg(order_key, ',') as order_numbers
  from max_3_orders
  group by 1 
),


last_highest_order as (
  select 
    customer_key,
    max(order_date) as last_order_date
  from max_3_orders
  group by 1 
),


parts_clean as (
  select 
    urgent_auto_customers.customer_key,
    replace(lineitem.l_partkey, ',', '') as part_key,
    lineitem.l_quantity as quantity,
    replace(lineitem.l_extendedprice, ',', '') as total_price,
    row_number() over (partition by urgent_auto_customers.customer_key  order by lineitem.l_extendedprice desc) as row_num
  from urgent_auto_customers 
  join snowflake_sample_data.TPCH_SF1.lineitem as lineitem 
    on lineitem.l_orderkey = urgent_auto_customers.order_key
),


-- pivot the parts per customer
pivot_parts as (
    select * from (
        select customer_key, part_key, row_num 
        from parts_clean) as parts 
    pivot ( 
        min(parts.part_key) for parts.row_num in (1,2,3)) 
    	as pivot_values (customer_key, part_1_key, part_2_key,part_3_key)   
),


-- same CTE as above but to pivot the quantites of the order per customer
pivot_quantities as (
    select * from (
        select customer_key, quantity, row_num 
        from parts_clean) as parts
    pivot ( 
        min(parts.quantity) for parts.row_num in (1,2,3))
        as pivot_values (customer_key, part_1_quantity, part_2_quantity, part_3_quantity)
),


-- same pivots as above but for totals 
pivot_totals as (
    select * from (
        select  customer_key, total_price, row_num 
        from parts_clean
    ) as parts
    pivot ( 
        min(parts.total_price) for parts.row_num in (1,2,3))
        as pivot_values (customer_key, part_1_total_spent, part_2_total_spent, part_3_total_spent)
),


final as (
  select 
    last_highest_order.customer_key,
    last_highest_order.last_order_date,
    combine_max_3_orders.total_spent,
    combine_max_3_orders.order_numbers,
    pivot_parts.part_1_key,
    pivot_quantities.part_1_quantity,
    pivot_totals.part_1_total_spent,
    pivot_parts.part_2_key,
    pivot_quantities.part_2_quantity,
    pivot_totals.part_2_total_spent,
    pivot_parts.part_3_key,
    pivot_quantities.part_3_quantity,
    pivot_totals.part_3_total_spent  
    from last_highest_order
  inner join combine_max_3_orders
    on last_highest_order.customer_key = last_highest_order.customer_key
  inner join pivot_parts 
    on pivot_parts.customer_key = last_highest_order.customer_key
  inner join pivot_quantities
    on pivot_quantities.customer_key = last_highest_order.customer_key
  inner join pivot_totals
    on pivot_totals.customer_key = last_highest_order.customer_key
)

select * from final order by last_order_date desc limit 100 


