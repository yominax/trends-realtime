import os, time, psycopg2, re
from collections import Counter, defaultdict
from datetime import datetime

DBH=os.getenv("DB_HOST","postgres"); DBN=os.getenv("DB_NAME","trends"); DBU=os.getenv("DB_USER","trends"); DBP=os.getenv("DB_PASS","trends"); DBPORT=int(os.getenv("DB_PORT","5432"))
STEP=int(os.getenv("STEP_SEC","30"))

TOKEN = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ']+")
CAP_SEQ = re.compile(r"\b(?:[A-Z][\wÀ-ÖØ-öø-ÿ'-]{2,}(?:\s+[A-Z][\wÀ-ÖØ-öø-ÿ'-]{2,})+)\b")
STOP = set("""
au aux du des les la le un une en sur pour par dans de d’ d' et ou mais si ne pas plus tres très
avec sans chez vers apres avant contre selon sous entre dont quand que qui quoi quel quels quelle quelles
l’ l' à a aux ces ce cet cette se son sa ses leur leurs nos vos ils elles on nous vous
""".split())

def connect():
    c = psycopg2.connect(host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT)
    c.autocommit=True
    return c

def toks(s):
    ws = [w.lower().strip("’'") for w in TOKEN.findall(s or "")]
    return [w for w in ws if w and w not in STOP and len(w)>2]

def top_phrases(cur, minutes, kind):
    cur.execute("""
      SELECT source, title
      FROM news_articles
      WHERE kind=%s AND published_ts >= NOW() - (%s || ' minutes')::interval
    """,[kind, minutes])
    uni=Counter(); bi=Counter(); ent=Counter(); srcmap=defaultdict(set)
    for source, title in cur.fetchall():
        w = toks(title or "")
        for p in CAP_SEQ.findall(title or ""):
            ent[p]+=1; srcmap[p].add(source)
        for t in w:
            uni[t]+=1; srcmap[t].add(source)
        for i in range(len(w)-1):
            bg=f"{w[i]} {w[i+1]}"; bi[bg]+=1; srcmap[bg].add(source)

    score=Counter()
    for k,v in uni.items(): score[k]+=v         # 1x
    for k,v in bi.items():  score[k]+=v*2       # 2x bigrams
    for k,v in ent.items(): score[k]+=v*3       # 3x capitalized entities

    rows=[]
    for phrase, sc in score.most_common(80):
        rows.append((phrase, int(sc), len(srcmap[phrase])))
    return rows

def write_entities(cur, rows, kind_tag):
    # Nettoyage fenêtre récente pour ce type
    cur.execute("DELETE FROM spikes_entities WHERE ts >= NOW()-interval '5 minutes' AND kind=%s", [kind_tag])
    for phrase, mentions, nb_src in rows:
        score = float(mentions + 0.7*nb_src)
        cur.execute("""
          INSERT INTO spikes_entities(ts, phrase, kind, mentions, sources, score)
          VALUES(NOW(), %s, %s, %s, %s, %s)
          ON CONFLICT (ts, phrase, kind) DO NOTHING
        """, (phrase, kind_tag, mentions, nb_src, score))

def main():
    conn=connect(); cur=conn.cursor()
    while True:
        # spikes_news (30 min) basé sur le flux continu
        cur.execute("""
          INSERT INTO spikes_news(ts, keyword, count_30m, score_norm)
          SELECT NOW(), k, c, c::float
          FROM (
            SELECT unnest(string_to_array(lower(regexp_replace(title,'[^A-Za-zÀ-ÖØ-öø-ÿ ]',' ','g')), ' ')) AS k,
                   COUNT(*) c
            FROM news_articles
            WHERE kind='continu' AND published_ts >= NOW() - interval '30 minutes'
            GROUP BY 1
            HAVING length(k)>2 AND k NOT IN %s
            ORDER BY c DESC
            LIMIT 100
          ) t
          ON CONFLICT DO NOTHING
        """, (tuple(STOP),))

        # Entités pour 60 min (continu) et 24 h (une)
        rows = top_phrases(cur, 60, 'continu')[:50]
        write_entities(cur, rows, 'news_continu')
        rows = top_phrases(cur, 1440, 'une')[:50]
        write_entities(cur, rows, 'news_une')

        time.sleep(STEP)

if __name__=="__main__":
    main()
