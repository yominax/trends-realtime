import os, time, psycopg2, pandas as pd
from datetime import datetime

DBH=os.getenv("DB_HOST","postgres"); DBN=os.getenv("DB_NAME","trends"); DBU=os.getenv("DB_USER","trends"); DBP=os.getenv("DB_PASS","trends"); DBPORT=int(os.getenv("DB_PORT","5432"))
STEP=int(os.getenv("STEP_SEC","60"))
WIKI_WIN=int(os.getenv("WIKI_WINDOW_MIN","15"))
HN_WIN=int(os.getenv("HN_WINDOW_MIN","30"))

def log(msg):
    print(f"{datetime.utcnow().isoformat()}Z [agg] {msg}", flush=True)

def connect():
    for _ in range(30):
        try:
            conn=psycopg2.connect(host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT)
            conn.autocommit=True; return conn
        except Exception as e:
            log(f"db not ready: {e}"); time.sleep(2)
    raise RuntimeError("db connect failed")

def norm_series(s):
    if s.empty:
        return s*0
    lo, hi = float(s.min()), float(s.max())
    if hi - lo < 1e-9:
        return s*0
    return (s - lo) / (hi - lo)

def compute_wiki(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT page, COUNT(*) edits
            FROM wiki_rc
            WHERE ts >= NOW() - INTERVAL '{WIKI_WIN} minutes'
            GROUP BY page
            HAVING COUNT(*) >= 2
            ORDER BY edits DESC
            LIMIT 50
        """)
        rows=cur.fetchall()
    if not rows:
        return
    df = pd.DataFrame(rows, columns=["page","edits"])
    df["score_norm"] = norm_series(df["edits"])
    ts = datetime.utcnow().replace(second=0, microsecond=0)
    with conn.cursor() as cur:
        for _, r in df.iterrows():
            cur.execute(
                """INSERT INTO spikes_wiki(ts,page,edits_15m,score_norm)
                   VALUES(%s,%s,%s,%s)
                   ON CONFLICT (ts,page) DO UPDATE SET
                     edits_15m=EXCLUDED.edits_15m,
                     score_norm=EXCLUDED.score_norm""",
                (ts, r["page"], int(r["edits"]), float(r["score_norm"]))
            )

def compute_hn(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT s.id, MIN(s.ts) min_ts, MAX(s.ts) max_ts,
                   MIN(s.score) min_score, MAX(s.score) max_score,
                   MAX(p.title) title, MAX(p.url) url,
                   MAX(p.descendants) comments, MAX(p.score) last_score
            FROM hn_scores s
            JOIN hn_posts p ON p.id = s.id
            WHERE s.ts >= NOW() - INTERVAL '{HN_WIN} minutes'
            GROUP BY s.id
            HAVING COUNT(*) >= 2
        """)
        rows=cur.fetchall()
    if not rows:
        return
    df = pd.DataFrame(rows, columns=["id","min_ts","max_ts","min_score","max_score","title","url","comments","last_score"])
    dur = (pd.to_datetime(df["max_ts"]) - pd.to_datetime(df["min_ts"])).dt.total_seconds()/60.0
    dur = dur.where(dur>0, other=1.0)
    df["velocity"] = (df["max_score"].astype(float) - df["min_score"].astype(float)) / dur
    df["velocity_norm"] = norm_series(df["velocity"])
    ts = datetime.utcnow().replace(second=0, microsecond=0)
    with conn.cursor() as cur:
        for _, r in df.iterrows():
            cur.execute(
                """INSERT INTO spikes_hn(ts,id,title,velocity,velocity_norm,score,comments,url)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (ts,id) DO UPDATE SET
                     title=EXCLUDED.title,
                     velocity=EXCLUDED.velocity,
                     velocity_norm=EXCLUDED.velocity_norm,
                     score=EXCLUDED.score,
                     comments=EXCLUDED.comments,
                     url=EXCLUDED.url""",
                (ts, int(r["id"]), r["title"], float(r["velocity"]), float(r["velocity_norm"]),
                 int(r["last_score"] or 0), int(r["comments"] or 0), r["url"])
            )
    import re, unicodedata
NEWS_WIN=int(os.getenv("NEWS_WINDOW_MIN","30"))

STOP_FR = set("""
au aux avec ce ces dans de des du elle en et eux il je la le leur lui ma mais me même mes moi mon ne nos notre nous on ou par pas pour qu que qui sa se ses son sur ta te tes toi ton tu un une vos votre vous c d j l à m n s t y été étée étés étées étant étais était étions étiez étaient être suis es est sommes êtes sont serai seras sera serons serez seront serais serait serions seriez seraient étais était étions étiez étaient fus fut fûmes fûtes furent sois soit soyons soyez soient sois soyez sont ai as a avons avez ont avais avait avions aviez avaient eut eûmes eûtes eurent
""".split())

def normalize(txt):
    if not txt: return ""
    txt = unicodedata.normalize("NFKD", txt).encode("ascii","ignore").decode("ascii")
    return txt.lower()

def tokenize_title(title):
    t = normalize(title)
    words = re.findall(r"[a-zA-Z]{3,}", t)
    return [w for w in words if w not in STOP_FR]

def compute_news(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT title
            FROM news_articles
            WHERE published_ts >= NOW() - INTERVAL '{NEWS_WIN} minutes'
        """)
        rows = cur.fetchall()
    if not rows: return
    from collections import Counter
    c = Counter()
    for (title,) in rows:
        for w in tokenize_title(title or ""):
            c[w] += 1
    # top 50
    items = c.most_common(50)
    counts = [cnt for _, cnt in items]
    if not counts: return
    lo, hi = min(counts), max(counts)
    def norm(v): 
        return 0.0 if hi==lo else (v - lo) / float(hi - lo)
    ts = datetime.utcnow().replace(second=0, microsecond=0)
    with conn.cursor() as cur:
        for kw, cnt in items:
            cur.execute(
                """INSERT INTO spikes_news(ts, keyword, count_30m, score_norm)
                   VALUES(%s,%s,%s,%s)
                   ON CONFLICT (ts,keyword) DO UPDATE SET
                     count_30m=EXCLUDED.count_30m,
                     score_norm=EXCLUDED.score_norm
                """,
                (ts, kw, int(cnt), float(norm(cnt)))
            )

# Dans main() boucle scheduler, après compute_wiki/compute_hn, ajoute :
# compute_news(conn)

# --- AJOUT DANS spike_aggregator.py ---
import re, unicodedata, math, os
WIN_MIN = int(os.getenv("ENTITIES_WINDOW_MIN", "60"))

STOP_FR = set("""
au aux avec ce ces dans de des du elle en et eux il je la le leur lui ma mais me même mes moi mon ne nos notre nous on ou par pas pour qu que qui sa se ses son sur ta te tes toi ton tu un une vos votre vous c d j l à m n s t y les aux auxi auxil a est sont été etais etait etions etiez etaient sera seront serai seras seront fut furent
""".split())

def _norm(t):
    t = unicodedata.normalize("NFKD", t or "").encode("ascii","ignore").decode("ascii")
    return re.sub(r"\s+", " ", t.lower()).strip()

def _tokens(title):
    words = re.findall(r"[a-z]{2,}", _norm(title))
    return [w for w in words if w not in STOP_FR]

def _bigrams(ws):
    return [" ".join(pair) for pair in zip(ws, ws[1:]) if len(pair[0])>2 and len(pair[1])>2]

def compute_entities(conn):
    # 1) charge les titres récents
    with conn.cursor() as cur:
        cur.execute(f"""
          SELECT 'news' AS src, title
          FROM news_articles
          WHERE published_ts >= NOW() - INTERVAL '{WIN_MIN} minutes'
          UNION ALL
          SELECT 'wiki' AS src, page
          FROM wiki_rc
          WHERE ts >= NOW() - INTERVAL '{WIN_MIN} minutes'
        """)
        rows = cur.fetchall()

    if not rows: return

    # 2) compte unigrams + bigrams et sources distinctes
    from collections import Counter, defaultdict
    cnt = Counter()
    srcs = defaultdict(set)

    for src, title in rows:
        ws = _tokens(title or "")
        grams = ws + _bigrams(ws)
        for g in grams:
            if len(g) < 3: continue
            cnt[g] += 1
            srcs[g].add(src)

    if not cnt: return
    # normalisation (0..1) + boost par nb de sources (mix > single)
    all_counts = list(cnt.values())
    lo, hi = min(all_counts), max(all_counts)
    def norm(v): return 0.0 if hi == lo else (v - lo) / float(hi - lo)

    now_ts = datetime.utcnow().replace(second=0, microsecond=0)
    with conn.cursor() as cur:
        for phrase, m in cnt.most_common(80):
            s_count = len(srcs[phrase])      # 1 = news/wiki seul ; 2 = mix
            kind = "mix" if s_count >= 2 else next(iter(srcs[phrase]))
            score = norm(m) * (1.0 + 0.25*(s_count-1))  # petit bonus multi-source
            cur.execute("""
              INSERT INTO spikes_entities(ts, phrase, kind, mentions, sources, score)
              VALUES (%s,%s,%s,%s,%s,%s)
              ON CONFLICT (ts,phrase,kind) DO UPDATE
              SET mentions=EXCLUDED.mentions, sources=EXCLUDED.sources, score=EXCLUDED.score
            """, (now_ts, phrase, kind, int(m), int(s_count), float(score)))

def main():
    conn = connect()
    while True:
        try:
            compute_wiki(conn)
            compute_hn(conn)
            compute_news(conn)
            compute_entities(conn)

        except Exception as e:
            log(f"agg error {e}")
        time.sleep(STEP)

if __name__=="__main__":
    main()
