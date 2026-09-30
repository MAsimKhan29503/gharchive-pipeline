import argparse
import gzip
import json
import os

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()  # reads your .env file

KEEP_TYPES = {"PushEvent", "PullRequestEvent", "WatchEvent", "IssuesEvent", "ForkEvent"}
BATCH_SIZE = 5000


def flush(cur, batch):
    execute_values(
        cur,
        """INSERT INTO raw.gh_events (event_id, event_type, created_at, source_file, payload)
           VALUES %s ON CONFLICT DO NOTHING""",
        batch,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="YYYY-MM-DD")
    parser.add_argument("hour", type=int, help="0-23")
    args = parser.parse_args()

    file_name = f"{args.date}-{args.hour}.json.gz"
    url = f"https://data.gharchive.org/{file_name}"
    print(f"Downloading {url} ...")

    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        user=os.environ["WH_USER"],
        password=os.environ["WH_PASSWORD"],
        dbname=os.environ["WH_DB"],
    )

    total = 0
    with conn, conn.cursor() as cur:
        # remove any rows from an earlier load of this same file
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

    conn.close()
    print(f"Loaded {total} rows from {file_name}")


if __name__ == "__main__":
    main()