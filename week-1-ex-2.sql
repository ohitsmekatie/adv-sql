

-- https://corise.com/course/advanced-sql/v2/module/project-1-instructions
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

customer_tags as (
    select 
        customer_survey.customer_id,
        trim(lower(recipe_tags.tag_property)) as tag_property,
        -- for ranking & counting in the pivot since we are only showing 3 
        row_number() over(partition by customer_survey.customer_id order by recipe_tags.tag_property) as tag_count
    from vk_data.customers.customer_survey 
    join vk_data.resources.recipe_tags 
        on customer_survey.tag_id = recipe_tags.tag_id
    --filtering on is_active to get the current taste preferences 
    where 
        customer_survey.is_active = true
    -- to get only the counts with <=3 from row_number above
    qualify tag_count <= 3
),


pivot_customer_tags as (
    select *
    from customer_tags
    -- using pivot from class example in lecture notes 
    pivot( max(tag_property) for tag_count in (1, 2, 3))
    as pivot_values (customer_id, tag_1, tag_2, tag_3)
),


flatten_recipes as(
   -- using flatten from class example in lecture notes 
   select
        recipe_id,
        recipe_name,
        trim(replace(flat_tag.value, '"', '')) as recipe_tag
    from vk_data.chefs.recipe, table(flatten(tag_list)) as flat_tag
),

max_recipe as (
    select 
        recipe_name,
        min(recipe_tag) as recipe_tag 
    from flatten_recipes 
    group by 1 
),

customers_w_recipe as (
    select
        distinct customer_id,
        min(recipe_name) as recipe_name
    from pivot_customer_tags 
    inner join max_recipe 
        on pivot_customer_tags.tag_1 = max_recipe.recipe_tag 
    group by 1
),

final as (
    select 
        customers.customer_id,
        customers.email,
        customers.first_name,
        pivot_customer_tags.tag_1 as food_pref_1,
        pivot_customer_tags.tag_2 as food_pref_2,
        pivot_customer_tags.tag_3 as food_pref_3,
        customers_w_recipe.recipe_name as suggested_recipe
    from customers_w_recipe
    inner join customers
        on customers_w_recipe.customer_id = customers.customer_id 
    inner join pivot_customer_tags
        on pivot_customer_tags.customer_id = customers.customer_id 
    order by first_name 
)

-- This is not quite right :D i'm not sure where i'm going wrong in the above but i'm going to submit this since it's the bonus exercise and i've been wracking my brain on it :D 
select * from final 
