
/*
This is simple example of dbt model to schedule in Fivetran, based on google sheet data.
*/

{{ config(materialized='table') }}


with skills as (
    select
        skill_group,
        round(avg(delivery), 2) as average_skill_value,
        count(*) as cnt_skilled
    from {{ source('google_sheets_bronze', 'skills_matrix_raw') }}
    where delivery <> 'NA'
    group by skill_group
)

select
    skill_group,
    average_skill_value,
    cnt_skilled
from skills
order by cnt_skilled desc

