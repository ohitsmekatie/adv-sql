
with customers as (
    -- 10k total customers
    select
        customer_data.customer_id,
        customer_data.first_name,
        customer_data.last_name,
        customer_data.email,
        address_id as customer_address_id,
        -- lowercasing city and states so they can be joined later and trimming to get rid of the secret (TRICKY!!! :D) whitespace at the beginning and end
        trim(lower(customer_city)) as customer_city,
        trim(lower(customer_state)) as customer_state_abbr
    from vk_data.customers.customer_data
    inner join vk_data.customers.customer_address
        on customer_data.customer_id = customer_address.customer_id
),

suppliers as (
    -- 10
    select
        supplier_id,
        supplier_name,
        -- lowercasing city and states so they can be joined later and adding trim just to be safe because of the other datasets
        trim(lower(supplier_city)) as supplier_city,
        trim(lower(supplier_state)) as supplier_state_abbr
    from vk_data.suppliers.supplier_info
),

geo as (
    select 
        -- lowercasing city and states so they can be joined later and adding trim just to be safe because of the other datasets
        trim(lower(city_name)) as city_name,
        trim(lower(state_abbr)) as state_abbr,
        geo_location
    from vk_data.resources.us_cities
),

suppliers_w_geo as (
    -- 10
    select
        suppliers.*,
        geo.geo_location as supplier_geo
    from suppliers
    inner join geo
        on suppliers.supplier_city = geo.city_name 
        and suppliers.supplier_state_abbr = geo.state_abbr 
),

customers_w_geo as (
    select 
        customers.*,
        geo.geo_location as customer_geo
    from customers 
    inner join geo 
        on customers.customer_city = geo.city_name
        and customers.customer_state_abbr = geo.state_abbr
),

calc_distance as (
    select
        *,
        -- returned in meters so you need to convert to miles
        (st_distance(customer_geo, supplier_geo) / 1609) as shipping_distance_miles
    from customers_w_geo
    cross join suppliers_w_geo
),

rank as (
    select
        *,
    rank() over (partition by customer_id order by shipping_distance_miles) as shipping_distance_ranked
    from calc_distance 
),

final as (
    select 
        customer_id,
        first_name,
        last_name,
        email,
        supplier_id,
        supplier_name,
        shipping_distance_miles
    from rank 
    where 
        shipping_distance_ranked = 1 
    order by last_name, first_name
)

select * from final
