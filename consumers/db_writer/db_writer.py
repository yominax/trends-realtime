import os, json, time, psycopg2
from kafka import KafkaConsumer
from datetime import datetime

BOOT=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:29092")
TOPIC_WIKI=os.getenv("TOPIC_WIKI","wiki_rc")
TOPIC_HN=os.getenv("TOPIC_HN","hn_posts")
TOPIC_NEWS=os.getenv("TOPIC_NEWS","news_fr")  # <-- ajout
DBH=os.getenv("DB_HOST","postgres"); DBN=os.getenv("DB_NAME","trends"); DBU=os.getenv("DB_USER","trends"); DBP=os.getenv("DB_PASS","trends"); DBPORT=int(os.getenv("DB_PORT","5432"))

def log(msg):
    print(f"{datetime.utcnow().isoformat()}Z [dbw] {msg}", flush=True)

def connect_db():
    for _ in range(30):
        try:
            conn=psycopg2.connect(host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT)
            conn.autocommit=True; return conn
        except Exception as e:
            log(f"db not ready: {e}"); time.sleep(2)
    raise RuntimeError("db connect failed")

def main():
    conn = connect_db()
    cur = conn.cursor()
    cons = KafkaConsumer(
        TOPIC_WIKI, TOPIC_HN,TOPIC_NEWS,
        bootstrap_servers=BOOT,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest", enable_auto_commit=True,
        group_id="db-writer", max_poll_records=100
    )
    for msg in cons:
        if msg.topic == TOPIC_WIKI:
            r = msg.value
            try:
                ts = datetime.utcfromtimestamp(int(r["ts"]))
            except Exception:
                ts = datetime.utcnow()
            cur.execute(
                "INSERT INTO wiki_rc(ts,page,user_name,comment,delta,url) VALUES(%s,%s,%s,%s,%s,%s)",
                (ts, r.get("page"), r.get("user"), r.get("comment"), int(r.get("delta") or 0), r.get("url"))
            )
        elif msg.topic == TOPIC_NEWS:
            r = msg.value
            ts_pub = datetime.utcfromtimestamp(int(r.get("published_ts") or time.time()))
            cur.execute(
                """INSERT INTO news_articles(published_ts, ts_ingest, source, title, url, summary)
                   VALUES(%s, NOW(), %s, %s, %s, %s)
                """,
                (ts_pub, r.get("source"), r.get("title"), r.get("url"), r.get("summary"))
            )
        else:
            r = msg.value
            ts = datetime.utcfromtimestamp(int(r.get("ts") or time.time()))
            cur.execute(
                """INSERT INTO hn_posts(id,ts,title,by_user,score,descendants,url)
                   VALUES(%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(id) DO UPDATE SET
                     title=EXCLUDED.title, by_user=EXCLUDED.by_user, score=EXCLUDED.score,
                     descendants=EXCLUDED.descendants, url=EXCLUDED.url""",
                (int(r["id"]), ts, r.get("title"), r.get("by"),
                 int(r.get("score") or 0), int(r.get("descendants") or 0), r.get("url"))
            )
            cur.execute(
                """INSERT INTO hn_scores(id,ts,score,comments) VALUES(%s,%s,%s,%s)
                   ON CONFLICT (id,ts) DO NOTHING""",
                (int(r["id"]), ts, int(r.get("score") or 0), int(r.get("descendants") or 0))
            )

if __name__=="__main__":
    main()
