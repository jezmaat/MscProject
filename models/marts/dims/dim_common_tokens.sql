{{ config(materialized='table', schema='dwh_qmul') }}

# remove special characters
with tokens as (
    select
        category,
        reference_date as date,
        lower(word) as token
    from {{ ref('stg_labelled_tweets') }},
    # regex to remove urls and mentions and links
    # it breaks each token into a different row
    unnest(split(regexp_replace(lower(tweet), r'http\S+|www\S+|@\w+|\d+|[^a-záàâãéèêíïóôõöúçñ\s]', ' '), ' ')) as word
    where tweet is not null
)
# remove stopwords. Only considers tokens with len > 3 
# List of stopwords is currently mannual. To be improved with a dim_stop_words in the future
, filtered as (
    select *
    from tokens
    where length(token) > 3
      and token not in ('pra','pro','tá','né','rt','vc','vai','aí','ser','ter','tão','url','username','number','http','https')
)
# counts frequency
, agg as (
    select
        category,
        date,
        token,
        count(*) as frequency,
        row_number() over(partition by category, date order by count(*) desc) as rnk
    from filtered
    group by 1,2,3
)
# selects top 40
select
    row_number() over(order by token) as token_id,
    token
from agg
where rnk <= 40
group by token
