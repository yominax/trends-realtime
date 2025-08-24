import os, time, pandas as pd, psycopg2, streamlit as st

DBH=os.getenv("DB_HOST","postgres"); DBN=os.getenv("DB_NAME","trends"); DBU=os.getenv("DB_USER","trends"); DBP=os.getenv("DB_PASS","trends"); DBPORT=int(os.getenv("DB_PORT","5432"))
REFRESH=int(os.getenv("REFRESH_SEC","10"))

st.set_page_config(page_title="Wikipedia — Trends Live", layout="wide")
st.title("🔥 Wikipedia — Trends Live")

@st.cache_resource
def conn():
    return psycopg2.connect(host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT)

def q(sql):
    with conn().cursor() as cur:
        cur.execute(sql); cols=[c[0] for c in cur.description]; rows=cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

col1, col2 = st.columns([2,1], gap="large")

with col1:
    st.subheader("Top sujets — dernière heure (Wikipedia)")
    wiki = q("""
      SELECT
        ts,
        ts AT TIME ZONE 'Europe/Paris' AS ts_local,
        page, edits_15m, score_norm
      FROM spikes_wiki
      ORDER BY ts DESC, score_norm DESC
      LIMIT 50
    """)
    st.subheader("Top mots-clés — 30 dernières minutes (Médias FR)")
    news = q("""
      SELECT ts, keyword, count_30m, score_norm
      FROM spikes_news
      ORDER BY ts DESC, score_norm DESC
      LIMIT 50
    """)
    if not news.empty:
        n = news.sort_values(["ts","score_norm"], ascending=[False,False]).head(15)
        st.bar_chart(n.set_index("keyword")["score_norm"])
    else:
        st.caption("Pas encore de mots-clés (attente des premiers articles).")
    if wiki.empty:
        st.info("En attente de données…")
    else:
        w = wiki.sort_values(["ts","score_norm"], ascending=[False,False]).head(10)
        st.bar_chart(w.set_index("page")["score_norm"])

    st.subheader("Éditions par minute — 60 dernières minutes (toutes pages)")
    # bucket 1 min des éditions
    edits_serie = q("""
      SELECT date_trunc('minute', ts AT TIME ZONE 'Europe/Paris') AS minute, COUNT(*) AS edits
      FROM wiki_rc
      WHERE ts >= NOW() - INTERVAL '60 minutes'
      GROUP BY 1
      ORDER BY 1
    """)
    if not edits_serie.empty:
        edits_serie = edits_serie.set_index("minute")
        st.line_chart(edits_serie["edits"])
    else:
        st.caption("Pas encore assez d'événements pour la série.")
    st.subheader("Tendances actuelles — 60 min (mots / phrases)")

entities = q("""
  SELECT ts, phrase, kind, mentions, sources, score
  FROM spikes_entities
  ORDER BY ts DESC, score DESC
  LIMIT 40
""")
if entities.empty:
    st.caption("En attente de données…")
else:
    # TOP 15, score décroissant
    e = entities.sort_values(["ts","score"], ascending=[False,False]).head(15)
    # Affichage barres (plus c'est gros, plus c'est important)
    st.bar_chart(e.set_index("phrase")["score"])
    # Petit tableau lisible
    st.dataframe(
        e[["phrase","kind","mentions","sources","score"]]
        .rename(columns={"kind":"source(s)", "mentions":"occurrences", "sources":"nb_sources"})
    )


with col2:
    st.subheader("Flux live (derniers événements Wikipedia)")
    last_wiki = q("""
      SELECT
        ts AT TIME ZONE 'Europe/Paris' AS ts_local,
        page, delta, url
      FROM wiki_rc
      ORDER BY ts DESC
      LIMIT 20
    """)

    if last_wiki.empty:
        st.caption("—")
    for _, r in last_wiki.iterrows():
        st.markdown(f"**[WIKI]** [{r['page']}]({r['url']}) — Δ {int(r['delta'])} — {r['ts_local']}")
    st.subheader("Flux live (Médias FR)")
    last_news = q("""
      SELECT 
        published_ts AT TIME ZONE 'Europe/Paris' AS ts_local,
        source, title, url
      FROM news_articles
      ORDER BY published_ts DESC
      LIMIT 20
    """)
    if last_news.empty:
        st.caption("—")
    for _, r in last_news.iterrows():
        st.markdown(f"**[{r['source']}]** [{r['title']}]({r['url']}) — {r['ts_local']}")

st.caption(f"Auto-refresh {REFRESH}s"); time.sleep(REFRESH); st.rerun()