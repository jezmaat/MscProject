{{ config(materialized='table', schema='dwh_qmul') }}

with agg as (
    select
        category,
        reference_date as date,
        lower(word) as token,
        # counts how many times the token has appeard
        count(*) as frequency,
        # for each group + date, sort tokens by frequency to calculate their ranking
        row_number() over(partition by category, reference_date order by count(*) desc) as rnk
    from {{ ref('stg_labelled_tweets') }},
    # breaks the text of tweet into different rows (one row for each token)
    # regex to remove urls and mentions and links
    unnest(split(regexp_replace(lower(tweet), r'http\S+|www\S+|@\w+|\d+|[^a-záàâãéèêíïóôõöúçñ\s]', ' '), ' ')) as word
    group by 1,2,3
)
select
    c.category_id,
    a.date,
    tok.token_id,
    a.frequency
from agg a
join {{ ref('dim_category') }} c
  on a.category = c.name
join {{ ref('dim_common_tokens') }} tok
  on a.token = tok.token
where a.rnk <= 40
