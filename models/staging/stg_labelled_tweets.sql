# intermediate area (staging)
{{ config(materialized='view') }}
with source as (
    select
        cast(tweet_id as int64) as tweet_id,
        cast(category as string) as category,
        cast(is_relevant as string) as is_relevant,
        cast(sentiment as string) as sentiment,
        cast(reference_date as date) as reference_date,
        cast(tweet as string) as tweet
    from {{ source('x_sentiment_analysis', 'labelled_tweets_human_rights_public') }}
)

select * from source