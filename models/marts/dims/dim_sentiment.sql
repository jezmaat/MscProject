{{ config(materialized='table', schema='dwh_qmul') }}
select distinct
    lower(trim(sentiment)) as sentiment,
    row_number() over(order by lower(trim(sentiment))) as sentiment_id
from {{ ref('stg_labelled_tweets') }}
where sentiment is not null