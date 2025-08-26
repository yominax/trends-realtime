import os
import json
import time
from typing import Iterable

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Configuration ---------------------------------------------------------
BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_NEWS = os.getenv("TOPIC_NEWS", "news_fr")
TOPIC_WIKI = os.getenv("TOPIC_WIKI", "wiki_rc")
TOPIC_HN = os.getenv("TOPIC_HN", "hn_posts")

DBH = os.getenv("DB_HOST", "postgres")
DBN = os.getenv("DB_NAME", "trends")
DBU = os.getenv("DB_USER", "trends")
DBP = os.getenv("DB_PASS", "trends")
DBPORT = int(os.getenv("DB_PORT", "5432"))

# --- Helpers ---------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


def connect_db():
    """Connect to Postgres with simple retry logic."""
    for i in range(20):
        try:
            conn = psycopg2.connect(
                host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError:
            time.sleep(0.5 + i * 0.2)
    # let it raise if still failing
    return psycopg2.connect(
        host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT
    )


def kafka_consumer(topics: Iterable[str]):
    """Create Kafka consumer with retry on broker availability."""
    while True:
        try:
            return KafkaConsumer(
                *topics,
                bootstrap_servers=BOOT,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
        except NoBrokersAvailable:
            log("Kafka non disponible → nouvel essai dans 5s")
            time.sleep(5)


# --- Write helpers ---------------------------------------------------------

def write_news(cur, rec: dict):
    cur.execute(
        """
        INSERT INTO news_articles(published_ts, source, title, url, summary)
        VALUES (to_timestamp(%s), %s, %s, %s, %s)
        """,
        (
            rec.get("published_ts"),
            rec.get("source"),
            rec.get("title"),
            rec.get("url"),
            rec.get("summary"),
        ),
    )


def write_wiki(cur, rec: dict):
    cur.execute(
        """
        INSERT INTO wiki_rc(ts, page, user_name, comment, delta, url)
        VALUES (to_timestamp(%s), %s, %s, %s, %s, %s)
        """,
        (
            rec.get("ts"),
            rec.get("page"),
            rec.get("user"),
            rec.get("comment"),
            rec.get("delta"),
            rec.get("url"),
        ),
    )


def write_hn(cur, rec: dict):
    cur.execute(
        """
        INSERT INTO hn_posts(id, ts, title, by_user, score, descendants, url)
        VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            ts = EXCLUDED.ts,
            title = EXCLUDED.title,
            by_user = EXCLUDED.by_user,
            score = EXCLUDED.score,
            descendants = EXCLUDED.descendants,
            url = EXCLUDED.url
        """,
        (
            rec.get("id"),
            rec.get("ts"),
            rec.get("title"),
            rec.get("by"),
            rec.get("score"),
            rec.get("descendants"),
            rec.get("url"),
        ),
    )
    cur.execute(
        """
        INSERT INTO hn_scores(id, ts, score, comments)
        VALUES (%s, to_timestamp(%s), %s, %s)
        ON CONFLICT (id, ts) DO NOTHING
        """,
        (
            rec.get("id"),
            rec.get("ts"),
            rec.get("score"),
            rec.get("descendants"),
        ),
    )


# --- Main loop -------------------------------------------------------------

HANDLERS = {
    TOPIC_NEWS: write_news,
    TOPIC_WIKI: write_wiki,
    TOPIC_HN: write_hn,
}


def main():
    conn = connect_db()
    cur = conn.cursor()
    consumer = kafka_consumer(HANDLERS.keys())
    for msg in consumer:
        handler = HANDLERS.get(msg.topic)
        if not handler:
            continue
        try:
            handler(cur, msg.value)
        except Exception as exc:
            log(f"Erreur {exc} pour topic {msg.topic}")


if __name__ == "__main__":
    main()
