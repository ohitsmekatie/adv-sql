-- when possible i like to predefine dates with variables for reusability
declare start_date date;
set start_date = date_trunc(date_sub(current_date, interval 90 day), week);

/*
imagine this is a rollup table of events that contain all the key actions
that have been predefined in terms of activity for a retention query
*/
with events as (
    select
        user_id,
        date_trunc(date(event_date), week) as event_week
    from event_rollup_table
    where date(created_at) >= start_date
),


users as (
    select
        user_id
        date_trunc(date(created_at), week) as signup_week,
    from users
    where
        date(created_at) >= start_date and

),


active_weeks as (
    select distinct
        users.user_id,
        users.signup_week,
        events.event_week,
        date_diff(events.event_week, users.signup_week, week) event_week_num
    from users
    left join events
        on users.user_id = events.user_id
)
/*
final query to get % of users who have one of the above events in a given time
period away from their signup date
*/
select
    signup_week,
    count(distinct actor_id) cohort_size,
    round((100 * countif(event_week_num = 0) / count(distinct actor_id)), 2) active_week_0,
    round((100 * countif(event_week_num = 1) / count(distinct actor_id)), 2) active_week_1,
    round((100 * countif(event_week_num = 2) / count(distinct actor_id)), 2) active_week_2,
    round((100 * countif(event_week_num = 3) / count(distinct actor_id)), 2) active_week_3,
    round((100 * countif(event_week_num = 4) / count(distinct actor_id)), 2) active_week_4,
    round((100 * countif(event_week_num = 5) / count(distinct actor_id)), 2) active_week_5,
    round((100 * countif(event_week_num = 6) / count(distinct actor_id)), 2) active_week_6,
    round((100 * countif(event_week_num = 7) / count(distinct actor_id)), 2) active_week_7,
    round((100 * countif(event_week_num = 8) / count(distinct actor_id)), 2) active_week_8,
    round((100 * countif(event_week_num = 9) / count(distinct actor_id)), 2) active_week_9,
    round((100 * countif(event_week_num = 10) / count(distinct actor_id)), 2) active_week_10,
    round((100 * countif(event_week_num = 11) / count(distinct actor_id)), 2) active_week_11,
    round((100 * countif(event_week_num = 12) / count(distinct actor_id)), 2) active_week_12,
from active_weeks
group by signup_week
order by signup_week
