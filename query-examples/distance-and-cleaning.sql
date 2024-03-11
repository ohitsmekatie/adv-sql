/* 
the approach i took for cleaning was to break things down into very small CTEs in the logical order of how i broke down the query
to keep things consistent i use all lowercase w/ trailng commas
*/


with customers as (
    select 
        customer_data.customer_id,
        customer_data.first_name || ' ' || customer_data.last_name as customer_name
    from vk_data.customers.customer_data
),


-- generally i like to clean data in different ctes than i'm using them so i've broken out this step into its own CTE
customer_addresses_clean as (
    select 
        customer_id,
        lower(trim(customer_city)) as customer_city,
        lower(trim(customer_state)) as customer_state 
    from vk_data.customers.customer_address
),


/* 
a cte using the cleaned up city & state from above to just get the impacted customers 
i think this is similar to what other folks were running into w/ the brownsville tx thing, but it's only returning 19 customers
*/
impacted_customers as (
    select * from customer_addresses_clean
    where 
        (customer_state = 'ky' and customer_city in ('concord','georgetown','ashland'))
         or (customer_state = 'ca' and customer_city in ('oakland','pleasant hill'))
         or (customer_state = 'tx' and customer_city in ('arlington','brownsville'))   
),


-- again, same as above i like to keep my cleaning in their own CTEs
cities_cleaned as (
    select
        lower(trim(city_name)) as city,
        lower(trim(state_abbr)) as state,
        geo_location
    from vk_data.resources.us_cities
),


chicago_geo as (
    select geo_location
    from cities_cleaned
    where 
        city = 'chicago' 
        and state = 'il'
),


gary_geo as (
    select geo_location
    from cities_cleaned
    where 
        city = 'gary'  
        and state = 'in'
),


customer_food_pref_count as (
    select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),


final as (
    select 
        customers.customer_name,
        impacted_customers.customer_city,
        impacted_customers.customer_state,
        customer_food_pref_count.food_pref_count,
        -- using round instead of casting to an integer to get the truncated number 
        round(st_distance(cities_cleaned.geo_location, chicago_geo.geo_location) / 1609) as chicago_dist_miles,
        round(st_distance(cities_cleaned.geo_location, gary_geo.geo_location) / 1609) as gary_dist_miles
    from customers 
    inner join impacted_customers 
        on customers.customer_id = impacted_customers.customer_id
    inner join customer_food_pref_count 
        on customers.customer_id = customer_food_pref_count.customer_id
    inner join cities_cleaned
        on cities_cleaned.city = impacted_customers.customer_city 
        and cities_cleaned.state = impacted_customers.customer_state 
    cross join chicago_geo
    cross join gary_geo       
)

select * from final
order by customer_name 
