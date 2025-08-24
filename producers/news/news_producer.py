import os, time, json, feedparser
from urllib.parse import urlparse
from datetime import datetime, timezone
from dateutil import parser as dtp
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOT  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("TOPIC", "news_fr")
POLL  = int(os.getenv("POLL_SEC", "30"))
# Feeds FR par défaut (tu peux en ajouter/enlever)
FEEDS = [u.strip() for u in os.getenv("FEEDS",
    ",".join([
      "https://www.lemonde.fr/rss/en_continu.xml",
      "https://www.bfmtv.com/rss/flux-actualites-a-la-une/",
      "https://www.france24.com/fr/rss",
      "https://www.lefigaro.fr/rss/feeds/figaro_actualites.xml",
      "https://www.leparisien.fr/actualites-a-la-une/rss.xml",
      "https://www.lesechos.fr/rss/rss_une.xml"
    ])
).split(",") if u.strip()]

def log(msg): print(datetime.now(timezone.utc).isoformat(), "[news]", msg, flush=True)

def to_ts(dt_str):
    if not dt_str:
        return int(time.time())
    try:
        return int(dtp.parse(dt_str).timestamp())
    except Exception:
        return int(time.time())

def kafka_producer_retry():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOT,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                retries=5
            )
        except NoBrokersAvailable:
            log("Kafka pas prêt → retry 5s"); time.sleep(5)

def main():
    prod = kafka_producer_retry()
    seen = set()
    while True:
        try:
            for url in FEEDS:
                feed = feedparser.parse(url)
                host = urlparse(url).netloc
                src  = host.replace("www.","")
                for e in feed.entries[:50]:
                    uid = e.get("id") or e.get("link")
                    if not uid or uid in seen:
                        continue
                    seen.add(uid)
                    rec = {
                        "source": src,
                        "title": e.get("title"),
                        "url": e.get("link"),
                        "summary": e.get("summary") or "",
                        "published_ts": to_ts(e.get("published") or e.get("updated")),
                        "ts_ingest": int(time.time())
                    }
                    prod.send(TOPIC, rec)
            time.sleep(POLL)
        except Exception as ex:
            log(f"error {ex} → sleep 5s"); time.sleep(5)

if __name__ == "__main__":
    main()
