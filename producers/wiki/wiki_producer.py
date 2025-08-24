import os, time, json, requests
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("TOPIC", "wiki_rc")
LANG = os.getenv("LANG", "fr")
POLL_SEC = int(os.getenv("POLL_SEC", "5"))
WIKI_DOMAIN = {"fr": "fr.wikipedia.org", "en": "en.wikipedia.org"}.get(LANG, "fr.wikipedia.org")

def log(msg): print(f"{datetime.now(timezone.utc).isoformat()} [wiki-rc] {msg}", flush=True)

def fetch(rcstart=None, rccontinue=None):
    url = f"https://{WIKI_DOMAIN}/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "list": "recentchanges",
        "rcprop": "title|user|comment|timestamp|sizes",
        "rcnamespace": "0",        # ,m
        "rctype": "edit|new",
        "rcshow": "!bot",          # ignore bots
        "rclimit": "200",
        "rcdir": "newer",
    }
    if rccontinue:
        params["rccontinue"] = rccontinue
    elif rcstart:
        params["rcstart"] = rcstart

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def kafka_producer_with_retry():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOT,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                retries=5,
            )
        except NoBrokersAvailable:
            log("Kafka pas prêt → nouvel essai dans 5s"); time.sleep(5)

def main():
    prod = kafka_producer_with_retry()

    # point de départ : 60s en arrière pour capter immédiatement des edits
    start_iso = (datetime.now(timezone.utc) - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rccont = None

    while True:
        try:
            data = fetch(rcstart=start_iso if not rccont else None, rccontinue=rccont)
            changes = data.get("query", {}).get("recentchanges", [])

            for rc in changes:
                newlen = rc.get("newlen") or 0
                oldlen = rc.get("oldlen") or 0
                delta = int(newlen - oldlen)
                ts = int(datetime.fromisoformat(rc["timestamp"].replace("Z","+00:00")).timestamp())

                rec = {
                    "page": rc.get("title"),
                    "ts": ts,
                    "user": rc.get("user"),
                    "comment": rc.get("comment"),
                    "delta": delta,
                    "url": f"https://{WIKI_DOMAIN}/wiki/{(rc.get('title') or '').replace(' ', '_')}"
                }
                prod.send(TOPIC, rec)

            # avancer le curseur
            rccont = data.get("continue", {}).get("rccontinue", rccont)
            # rythme court = quasi temps réel
            time.sleep(POLL_SEC if changes else max(POLL_SEC, 3))
        except Exception as e:
            log(f"error {e} → retry 5s"); time.sleep(5)

if __name__ == "__main__":
    main()
