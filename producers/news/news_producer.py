import os, json, time, hashlib, feedparser, requests
from urllib.parse import urlparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOT        = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC       = os.getenv("TOPIC", "news_fr")
POLL_SEC    = int(os.getenv("POLL_SEC", "20"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "12"))
FEEDS_ENV   = os.getenv("FEEDS", "")
FEEDS_FILE  = os.getenv("FEEDS_FILE", "/app/feeds.txt")

UA = "TrendsRealtimeBot/1.0 (+github.com/yominax/trends-realtime; contact: you@example.com)"
HDRS = {
    "User-Agent": UA,
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7",
}

def log(m): print(f"{datetime.now(timezone.utc).isoformat()} [news] {m}", flush=True)

def read_feeds():
    feeds=[]
    if FEEDS_ENV.strip():
        feeds += [u.strip() for u in FEEDS_ENV.split(",") if u.strip()]
    try:
        with open(FEEDS_FILE, "r", encoding="utf-8") as f:
            feeds += [l.strip() for l in f if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        pass
    out=[]; seen=set()
    for u in feeds:
        if u not in seen: out.append(u); seen.add(u)
    return out

def kafka_producer_with_retry():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOT,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                retries=5
            )
        except NoBrokersAvailable:
            log("Kafka pas prêt → retry 5s"); time.sleep(5)

def norm_ts(entry):
    for k in ("published_parsed","updated_parsed"):
        v = getattr(entry, k, None)
        if v:
            return int(datetime(*v[:6], tzinfo=timezone.utc).timestamp())
    return int(datetime.now(timezone.utc).timestamp())

def fetch_bytes(url, timeout=20):
    # suit les redirections proprement (évite les boucles 30x de feedparser)
    r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.content, r.headers.get("Content-Type","")

def pull_feed(url, seen_hashes):
    src = urlparse(url).netloc.replace("www.","")
    out = []
    try:
        body, ctype = fetch_bytes(url)
        # ne pas rejeter si le serveur renvoie text/html alors que c'est un RSS valide
        d = feedparser.parse(body)
        if d.bozo or not getattr(d, "entries", None):
            return src, out, f"bozo={getattr(d,'bozo_exception',None)}"
        for e in d.entries[:100]:
            key = hashlib.md5((e.get("link","")+e.get("title","")).encode("utf-8")).hexdigest()
            if key in seen_hashes: 
                continue
            seen_hashes.add(key)
            out.append({
                "published_ts": norm_ts(e),
                "source": src,
                "title": (e.get("title","") or "").strip(),
                "url": e.get("link","") or "",
                "summary": (e.get("summary","") or "")[:600]
            })
        if len(seen_hashes) > 3000:
            seen_hashes.clear()
        return src, out, None
    except Exception as ex:
        return src, out, str(ex)

def main():
    prod  = kafka_producer_with_retry()
    feeds = read_feeds()
    log(f"{len(feeds)} flux RSS chargés")

    last_hash = {u: set() for u in feeds}

    while True:
        pushed_total = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(pull_feed, u, last_hash[u]): u for u in feeds}
            for fut in as_completed(futures):
                src, recs, err = fut.result()
                if err:
                    log(f"{src} invalide: {err}")
                    continue
                for r in recs:
                    prod.send(TOPIC, r)
                pushed_total += len(recs)
        if pushed_total:
            log(f"+{pushed_total} articles")
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
