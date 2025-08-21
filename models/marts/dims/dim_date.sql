{{ config(materialized='table', schema='dwh_qmul') }}
select distinct
    reference_date as date,
    extract(year from reference_date) as year,
    extract(month from reference_date) as month,
    extract(day from reference_date) as day,
    format_date('%A', reference_date) as day_of_week, #%A for day of the week (Thursday, etc)
    extract(isoweek from reference_date) as week_of_year
from {{ ref('stg_labelled_tweets') }}
where reference_date is not null
