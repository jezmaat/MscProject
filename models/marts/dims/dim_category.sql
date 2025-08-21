{{ config(materialized='table', schema='dwh_qmul') }}
select distinct
    category as name,
    row_number() over(order by category) as category_id
from {{ ref('stg_labelled_tweets') }}
where category is not null