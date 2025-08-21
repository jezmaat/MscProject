FROM apache/airflow:2.10.2-python3.11

USER root
# update ubuntu packages and install gcc compiler (to use python commands)
#libpq-dev is for postgres, to use psycopg2
# it also removes cache to reduce size of image
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# installing requirements
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
