# Fact table (factless) to store tweets classifications
# Level-of-granularity: category (group), sentiment and flag for relevance
# The field 'is_valid' is used for synchronization to check if a tweet is still valid

{{ config(materialized='table', schema='dwh_qmul') }}
select
    t.tweet_id,
    c.category_id,
    t.reference_date as date,
    s.sentiment_id,
    case
        when trim(t.is_relevant) = 'Sim' then true
        when trim(t.is_relevant) = 'Não' then false
        else null
    end as is_relevant,
    true as is_valid
from {{ ref('stg_labelled_tweets') }} t
left join {{ ref('dim_category') }} c
  on t.category = c.name
left join {{ ref('dim_sentiment') }} s
  on lower(trim(t.sentiment)) = s.sentiment
