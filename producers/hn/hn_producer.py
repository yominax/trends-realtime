# producers/hn/hn_producer.py
import os, time, json, requests
from datetime import datetime
from kafka import KafkaProducer

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("TOPIC", "hn_posts")
POLL = int(os.getenv("POLL_SEC", "30"))
TOPN = int(os.getenv("TOP_N", "100"))

API = "https://hacker-news.firebaseio.com/v0"

def get(url):
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def fetch_item(i):
    d = get(f"{API}/item/{i}.json")
    if not d or d.get("type") != "story":
        return None
    return {
        "id": d.get("id"),
        "ts": d.get("time"),
        "title": d.get("title"),
        "by": d.get("by"),
        "score": d.get("score") or 0,
        "descendants": d.get("descendants") or 0,
        "url": d.get("url"),
    }

def main():
    prod = KafkaProducer(
        bootstrap_servers=BOOT,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=5,
    )
    seen = set()
    while True:
        try:
            # 1) Top stories (flux principal)
            ids = get(f"{API}/topstories.json")[:TOPN]
            for i in ids:
                it = fetch_item(i)
                if it:
                    prod.send(TOPIC, it)
                    seen.add(i)

            # 2) Rafraîchir les scores récents (quasi temps réel)
            upd = get(f"{API}/updates.json").get("items", [])[:200]
            for i in upd:
                if i in seen:  # on ne spamme pas toute la base
                    it = fetch_item(i)
                    if it:
                        prod.send(TOPIC, it)

            time.sleep(POLL)
        except Exception as e:
            print(f"{datetime.utcnow().isoformat()}Z [hn] error {e}; sleep 5s", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
