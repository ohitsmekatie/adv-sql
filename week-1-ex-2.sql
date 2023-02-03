
with customers as (
    select 
        customer_data.customer_id,
        customer_data.first_name,
        customer_data.last_name, 
        customer_data.email, 
        address_id as customer_address_id,
        trim(lower(customer_city)) as customer_city,
        trim(lower(customer_state)) as customer_state_abbr
    from vk_data.customers.customer_data
    inner join vk_data.customers.customer_address 
        on customer_data.customer_id = customer_address.customer_id 
),

geo as (
    select 
        trim(lower(city_name)) as city_name,
        trim(lower(state_abbr)) as state_abbr,
        geo_location
    from vk_data.resources.us_cities
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

-- evertyhing above this was a reused CTE from exercise.1 

eligible_survey_customers as (
    select 
        customers_w_geo.customer_id,
        customers_w_geo.first_name,
        customers_w_geo.email,
        tag_id 
    from customers_w_geo
    inner join vk_data.customers.customer_survey
        on customers_w_geo.customer_id = customer_survey.customer_id 
    where 
        customer_survey.is_active = True 
),

customer_tags as (
    select 
        eligible_survey_customers.customer_id,
        trim(lower(recipe_tags.tag_property)) as tag_property,
        -- for ranking & counting in the pivot since we are only showing 3 
        row_number() over(partition by eligible_survey_customers.customer_id order by recipe_tags.tag_property) as tag_count
    from eligible_survey_customers 
    join vk_data.resources.recipe_tags 
        on eligible_survey_customers.tag_id = recipe_tags.tag_id
    -- to get only the counts with <=3 from row_number above
    qualify tag_count <= 3
),

pivoted_customer_tags as (
    select *
    from customer_tags
    -- using pivot from class example in lecture notes 
    pivot( max(tag_property) for tag_count in (1, 2, 3))
    as pivot_values (customer_id, food_pref_1, food_pref_2, food_pref_3 )
),

flatten_recipes as(
   -- using flatten from class example in lecture notes 
   select
        recipe_id,
        recipe_name,
        trim(replace(flat_tag.value, '"', '')) as recipe_tag
    from vk_data.chefs.recipe, table(flatten(tag_list)) as flat_tag
),

get_single_recipe as (
    select 
        recipe_tag,
        max(recipe_name) as recipe_name
    from flatten_recipes 
    group by 1 
),

final as (
    select 
        distinct pivoted_customer_tags.customer_id,
        eligible_survey_customers.email,
        eligible_survey_customers.first_name,
        pivoted_customer_tags.food_pref_1,
        pivoted_customer_tags.food_pref_2,
        pivoted_customer_tags.food_pref_3,
        get_single_recipe.recipe_name as suggested_recipe
    from pivoted_customer_tags 
    inner join get_single_recipe 
        on pivoted_customer_tags.food_pref_1 = get_single_recipe.recipe_tag 
    inner join eligible_survey_customers
        on eligible_survey_customers.customer_id = pivoted_customer_tags.customer_id 
)

select * from final 
order by email 
