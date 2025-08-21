# Posts from Twitter to BigQuery Pipeline

This is an Airflow project pipeline to extract tweets about human rights and load them into Google BigQuery.

## Requirements

- Docker & Docker Compose  
- Google Cloud Project and BigQuery  
- Twitter API Bearer Token

## Setup

- Copy `.env.example` to `.env` and add your credentials. Example:

```env
AIRFLOW_UID=50000
TWITTER_BEARER_TOKEN=your_token # insert your API token here in case you wanna test it
GCP_PROJECT_ID=iguassu-data-lab # example
BQ_DATASET=mdhc
BQ_TABLE=tweets
KEYWORDS_FILE=/opt/airflow/dags/keywords.json # check if the name matches your key
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp-key.json
AIRFLOW__CORE__FERNET_KEY=YOUR_KEY # change here
```

- Put your GCP key in `gcp-key.json` (path should match `GOOGLE_APPLICATION_CREDENTIALS`).
- Build and start the Docker containers:

```bash
docker-compose up --build
```

- Airflow webserver will be running at `http://localhost:8080`.
- DAGs are in the `dags/` folder.

## How it works

- The pipeline reads keywords from `keywords.json` (you can add more or delete them as you wish).  
- It searches recent tweets using the Twitter API.  
- Removes accents and cleans text.  
- Loads tweets into the BigQuery table.  
- Supports incremental updates using the last tweet ID.

## Files

- `.env` — environment variables  
- `docker-compose.yml` — Docker services  
- `Dockerfile` — custom Airflow image  
- `requirements.txt` — Python dependencies  
- `dags/` — contains the main DAG script
