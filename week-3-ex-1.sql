-- creating an unpacked events CTE to be used throughout the rest of the query 
with events as (
	select 
    	event_id,
        session_id,
        user_id,
        event_timestamp,
        -- https://docs.snowflake.com/en/sql-reference/functions/parse_json
        -- cast as a string to remove quotes from extracting the json 
        parse_json(event_details):event::string as event,
        parse_json(event_details):recipe_id::string as recipe_id        
    from vk_data.events.website_activity 
    -- for de-duplicating events 
    group by 1,2,3,4,5,6
),

-- for each session_id, getting the min/max for start & end times to use as average later 
daily_session_metrics as (
	select 
    	date(event_timestamp) as event_day,
    	session_id,
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        timestampdiff('seconds', session_start, session_end) as session_length_in_seconds,
        -- using nullifzero so that i can divide these later to get the ratio of search to view per day. division by 0 error otherwise 
        nullifzero(count_if(event = 'search')) as num_search_events,
        nullifzero(count_if(event = 'view_recipe' )) as num_recipe_views 
    from events 
    group by 1,2
),

-- gets the top recipe per day with the total number of views 
daily_top_recipe as (
	select 
    	date(event_timestamp) as event_day,
    	recipe_id,
        count(*) as num_views
    from events 
    where recipe_id is not null 
    group by 1,2
    /*
    this was a really handy way to get the top recipe wihtout duplicate rank that someone in the class suggested
    i did not know about qualify before this class and it's pretty handy!
    */
    qualify row_number() over (partition by event_day order by num_views desc) = 1
),

-- decided to calculate all the metrics together in 1 CTE to group CTEs by "purpose"
calculate_metrics as (
    select 
    	daily_session_metrics.event_day,
        -- count distinct session_id to get the unique session visits 
        count(distinct daily_session_metrics.session_id) as num_unique_sessions,
        -- avg time different (using timestampdiff) from above CTE
        round(avg(daily_session_metrics.session_length_in_seconds),2) as avg_session_length,
        -- i'm not sure if i'm interpreting the question correctly but taking the avg num search events to recipe views to get avg ratio for the day
        avg(num_search_events / num_recipe_views) as avg_search_to_recipe, 
        -- top recipe id from above w/ max for aggregate 
        max(daily_top_recipe.recipe_id) as top_recipe_id
    from daily_session_metrics
    inner join daily_top_recipe 
    	on daily_session_metrics.event_day = daily_top_recipe.event_day 
    group by 1 
)

-- final query bringing in the recipe name to be a little more friednly to the eyes 
-- i decided to remove this because it was adding complexity to the query and run time but leaving for documentation sake 
-- final as (
-- 	select 
--     	calculate_metrics.*,
--         recipe.recipe_name 
--     from calculate_metrics 
--     inner join vk_data.chefs.recipe
--     	on calculate_metrics.top_recipe_id = recipe.recipe_id 
-- )

select * from calculate_metrics 
order by event_day 
