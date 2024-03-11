-- a few snippets with sensitive columns/tables changed of metrics snapshots i set up in dbt


-- DAILY ACTIVE DATABASES
-- https://docs.getdbt.com/docs/build/snapshots#timestamp-strategy-recommended

{% snapshot metrics_snp__daily_active_db_states %}

    {{
        config(
          target_project='project-analytics',
          target_dataset='snapshots',
          strategy='timestamp',
          unique_key='day',
          updated_at='day'
        )
    }}

    select
        current_date() as day,
        billing.plan,
        databases.state,
        -- business logic to count only active databses
        count(distinct(case when databases.deleted_at is null and state = 'ready' then databases.id end)) as total_active_databases
    from {{ source('analytics', 'databases')}}
    inner join {{ source('analytics', 'billing')}}
        on databases.id = billing.billable_id
    where
        actor_id not in (select user_id from {{ ref('stg_snapshots__staff_users')}})
        and billing.billable_type is not null
        and billing.billable_type in ('Database')
    group by 1,2,3

{% endsnapshot %}


-- DAILY DATABASES

-- https://docs.getdbt.com/docs/build/snapshots#timestamp-strategy-recommended

{% snapshot metrics_snp__daily_databases %}

    {{
        config(
          target_project='analytics',
          target_dataset='snapshots',
          strategy='timestamp',
          unique_key='day',
          updated_at='day'
        )
    }}

    select
        current_date() as day,
        billing.plan,
        -- business logic
        count(distinct(case when databases.deleted_at is null and state = 'ready' then databases.id end)) as total_active_databases,
        count(distinct(case when databases.deleted_at is null then databases.id end)) as total_non_deleted_databases,
        count(distinct databases.id) as total_databases
    from {{ source('analytics', 'databases')}}
    inner join {{ source('analytics', 'billing')}}
        on databases.id = billing.billable_id
    where
        actor_id not in (select user_id from {{ ref('stg_snapshots__staff_users') }})
        and billing.billable_type is not null
        and billing.billable_type in ('Database')
    group by 1,2

{% endsnapshot %}

## USERS

-- https://docs.getdbt.com/docs/build/snapshots#timestamp-strategy-recommended

{% snapshot metrics_snp__daily_users %}

    {{
        config(
          target_project='analytics',
          target_dataset='snapshots',
          strategy='timestamp',
          unique_key='day',
          updated_at='day'
        )
    }}

    select
        current_date() as day,
        -- business logic
        count(distinct(case when deleted_at is null and confirmed_at is not null then id end)) as total_confirmed_users,
        count(distinct(case when deleted_at is null then id end)) as total_non_deleted_users,
        count(distinct(id)) as total_users
    from {{ source('analytics', 'user') }}
    where
        id not in (select user_id from {{ ref('stg_snapshots__staff_users') }})
    group by 1

{% endsnapshot %}