import gzip
import json
import os
from datetime import datetime, timedelta

import psycopg2
import requests
from psycopg2.extras import execute_values

from airflow.decorators import dag, task
from airflow.datasets import Dataset

KEEP_TYPES = {"PushEvent", "PullRequestEvent", "WatchEvent", "IssuesEvent", "ForkEvent"}
BATCH_SIZE = 5000
RAW_EVENTS = Dataset("postgres://warehouse-db/raw.gh_events")


def flush(cur, batch):
    execute_values(
        cur,
        """INSERT INTO raw.gh_events (event_id, event_type, created_at, source_file, payload)
           VALUES %s ON CONFLICT DO NOTHING""",
        batch,
    )


@dag(
    dag_id="gharchive_ingest",
    start_date=datetime(2026, 10, 1),       # UTC; backfills from this date onward
    schedule="@hourly",
    catchup=True,
    max_active_runs=2,
    default_args={"retries": 6, "retry_delay": timedelta(minutes=10)},
    tags=["gharchive", "ingest"],
)
def gharchive_ingest():
    @task(outlets=[RAW_EVENTS])
    def load_hour(data_interval_start=None):
        file_name = f"{data_interval_start:%Y-%m-%d}-{data_interval_start.hour}.json.gz"
        url = f"https://data.gharchive.org/{file_name}"
        print(f"Downloading {url}")

        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        conn = psycopg2.connect(
            host=os.environ["WH_HOST"],
            port=os.environ["WH_PORT"],
            user=os.environ["WH_USER"],
            password=os.environ["WH_PASSWORD"],
            dbname=os.environ["WH_DB"],
        )

        total = 0
        try:
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM raw.gh_events WHERE source_file = %s", (file_name,))

                batch = []
                with gzip.GzipFile(fileobj=resp.raw) as gz:
                    for line in gz:
                        event = json.loads(line)
                        if event["type"] not in KEEP_TYPES:
                            continue
                        batch.append(
                            (int(event["id"]), event["type"], event["created_at"],
                             file_name, json.dumps(event))
                        )
                        if len(batch) >= BATCH_SIZE:
                            flush(cur, batch)
                            total += len(batch)
                            batch = []
                if batch:
                    flush(cur, batch)
                    total += len(batch)
        finally:
            conn.close()

        print(f"Loaded {total} rows from {file_name}")
        return {"file": file_name, "rows": total}

    load_hour()


gharchive_ingest()