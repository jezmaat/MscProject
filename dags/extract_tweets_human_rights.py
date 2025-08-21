from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import os
import json
import requests
import pandas as pd
import unicodedata

BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
TABLE = os.getenv("BQ_TABLE")

# function to remove accent from words
def remove_accents(text):
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

# function to build the twitter query with keywords
def build_query(terms):
    joined = ' OR '.join(terms)
    return f'({joined}) lang:pt -is:retweet -is:reply'

# function to search tweets in all pages
def search_all_pages(query, since_id=None, max_pages=10):
    tweets, next_token = [], None
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    for _ in range(max_pages):
        params = {
            "query": query,
            "max_results": 100,
            "tweet.fields": "created_at,lang,public_metrics,text"
        }
        if next_token:
            params["next_token"] = next_token
        if since_id:
            params["since_id"] = since_id
        resp = requests.get(SEARCH_URL, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        tweets.extend(data.get("data", []))
        next_token = data.get("meta", {}).get("next_token")
        if not next_token:
            break
    return tweets

# function to load keywords from file
def load_keywords():
    with open(os.getenv("KEYWORDS_FILE", "/opt/airflow/dags/keywords.json"), "r", encoding="utf-8") as f:
        return json.load(f)

# function to get the most recent tweet id in BigQuery
def get_recent_max_id(category):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT MAX(id) as max_id
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE category = @cat
          AND tweet_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 DAY)
    """
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("cat", "STRING", category)]
    ))
    rows = list(job.result())
    return rows[0].max_id if rows and rows[0].max_id else None

# function to extract tweets and insert to BigQuery
def extract_and_store_tweets(category, terms, full_resync=False):
    since_id = None if full_resync else get_recent_max_id(category)
    tweets = search_all_pages(build_query(terms), since_id=since_id)
    if not tweets:
        return
    rows = []
    for t in tweets:
        metrics = t["public_metrics"]
        rows.append({
            "id": int(t["id"]),
            "tweet_date": pd.to_datetime(t["created_at"]),
            "text": t["text"].replace("\n", " ").strip(),
            "likes": metrics["like_count"],
            "retweets": metrics["retweet_count"],
            "category": category,
            "extraction_date": datetime.now()
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["id"])
    if df.empty:
        return
    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(
        df, f"{PROJECT_ID}.{DATASET}.{TABLE}"
    )
    job.result()

# function to delete missing tweets in monthly resync
def clean_deleted_tweets():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE tweet_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 DAY)
        AND id NOT IN (
            SELECT id FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        )
    """
    client.query(query).result()

# function to run the daily incremental pipeline
def run_pipeline():
    categories = load_keywords()
    for category, terms in categories.items():
        expanded = list(set(terms + [remove_accents(t) for t in terms]))
        extract_and_store_tweets(category, expanded, full_resync=False)

# function to run the monthly resync pipeline
def run_pipeline_full():
    categories = load_keywords()
    for category, terms in categories.items():
        expanded = list(set(terms + [remove_accents(t) for t in terms]))
        extract_and_store_tweets(category, expanded, full_resync=True)
    clean_deleted_tweets()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# daily incremental dag
with DAG(
    "extract_tweets_human_rights_daily",
    default_args=default_args,
    description="This function is responsible for daily incremental extraction of tweets",
    schedule_interval="0 3 * * *", # runs every day at 3 am (* * * for any day, month and day of the week)
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["twitter", "human_rights"]
) as dag_daily:

    extract_task = PythonOperator(
        task_id="extract_and_load_daily",
        python_callable=run_pipeline
    )

# monthly full resync dag
with DAG(
    "extract_tweets_human_rights_monthly",
    default_args=default_args,
    description="This function is responsible for full extraction and cleaning of tweets according to X's policies",
    schedule_interval="@monthly",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["twitter", "human_rights"]
) as dag_monthly:

    extract_full_task = PythonOperator(
        task_id="extract_and_load_monthly",
        python_callable=run_pipeline_full
    )
