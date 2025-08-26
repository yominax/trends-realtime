import os, time, re, collections
import pandas as pd
import psycopg2, streamlit as st
from io import BytesIO
from wordcloud import WordCloud, STOPWORDS
from PIL import Image

# ---------- Config ----------
DBH = os.getenv("DB_HOST", "postgres")
DBN = os.getenv("DB_NAME", "trends")
DBU = os.getenv("DB_USER", "trends")
DBP = os.getenv("DB_PASS", "trends")
DBPORT = int(os.getenv("DB_PORT", "5432"))
REFRESH = int(os.getenv("REFRESH_SEC", "60"))

st.set_page_config(page_title="Trends Live — Médias FR", layout="wide")

st.markdown(
    """
    <style>
    .stApp {
        background-color: #000;
        background-image: radial-gradient(#fff 1px, transparent 1px), radial-gradient(#fff 1px, transparent 1px);
        background-position: 0 0, 25px 25px;
        background-size: 50px 50px;
        color: #e0e0e0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.image("https://upload.wikimedia.org/wikipedia/commons/9/91/Octicons-rss-16.svg", width=60)
st.title("Trends Live — Médias FR")

# ---------- Connexion DB (avec retry) ----------
@st.cache_resource
def conn():
    for i in range(25):
        try:
            c = psycopg2.connect(host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT)
            c.autocommit = True
            return c
        except Exception:
            time.sleep(0.5 + i*0.2)
    # laisse remonter l'erreur si jamais
    return psycopg2.connect(host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT)

def q(sql, params=None):
    with conn().cursor() as cur:
        cur.execute(sql, params or ())
        cols=[c[0] for c in cur.description]; rows=cur.fetchall()
    return pd.DataFrame(rows, columns=cols)



FR_STOP = STOPWORDS.union({
    # articles / pronoms / prépositions courantes
    "le","la","les","des","du","de","d","d’","d'","un","une","en","sur","pour","par","dans","avec","sans",
    "et","ou","mais","donc","or","ni","car","ne","pas","plus","très","ainsi","comme","lors","chez","vers",
    "au","aux","ce","cet","cette","ces","se","son","sa","ses","leur","leurs","nos","vos","oui","non",
    "il","elle","ils","elles","on","nous","vous","qui","que","quoi","qu","c","l","y","à","a","deux",
    # jours/mois
    "lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche",
    "janvier","février","mars","avril","mai","juin","juillet","août","septembre","octobre","novembre","décembre"
})
TOKEN = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ'-]+")
CAP_SEQ = re.compile(r"\b(?:[A-Z][\wÀ-ÖØ-öø-ÿ'-]{2,}(?:\s+[A-Z][\wÀ-ÖØ-öø-ÿ'-]{2,})+)\b")


def toks(s: str):
    """Nettoie une chaîne en :
       - conservant les sigles en MAJ (AI, UE, OTAN…)
       - baissant le reste
       - retirant stopwords/petits mots
    """
    out = []
    for raw in TOKEN.findall(s or ""):
        t = raw.strip("’'")
        if not t:
            continue
        if t.isupper() and 2 <= len(t) <= 5:
            norm = t                 # conserve les sigles : AI, UE, ONU…
        else:
            norm = t.lower()
        if norm in FR_STOP or len(norm) <= 2:
            continue
        out.append(norm)
    return out

def compute_trends_df(titles:list[str]):
    if not titles:
        return pd.DataFrame(columns=["phrase","score"])
    uni=collections.Counter(); bi=collections.Counter(); ent=collections.Counter()
    for t in titles:
        for p in CAP_SEQ.findall(t or ""): ent[p]+=1
        w=toks(t or "")
        uni.update(w)
        for i in range(len(w)-1): bi.update([w[i]+" "+w[i+1]])
    scores=collections.Counter()
    for k,v in uni.items(): scores[k]+=v
    for k,v in bi.items():  scores[k]+=v*2
    for k,v in ent.items(): scores[k]+=v*3
    items=scores.most_common(50)
    return pd.DataFrame(items, columns=["phrase","score"])


def wc_from_titles(titles:list[str]):
    if not titles: return None
    text=" ".join(titles)
    wc=WordCloud(width=1000, height=360, background_color="white", collocations=False, stopwords=FR_STOP).generate(text)
    buf=BytesIO(); wc.to_image().save(buf, format="PNG"); buf.seek(0)
    return Image.open(buf)

# ---------- Requêtes utilitaires ----------
def news_since(minutes:int, kind:str):
    return q("""
      SELECT published_ts AT TIME ZONE 'Europe/Paris' AS ts_local, source, title, url
      FROM news_articles
      WHERE kind=%s AND published_ts >= NOW() - (%s || ' minutes')::interval
      ORDER BY published_ts DESC
    """,[kind, minutes])

def last_news(n:int=30, kind:str="une"):
    return q("""
      SELECT published_ts AT TIME ZONE 'Europe/Paris' AS ts_local, source, title, url
      FROM news_articles
      WHERE kind=%s
      ORDER BY published_ts DESC
      LIMIT %s
    """,[kind, n])

def wiki_last(n:int=20):
    return q("""
      SELECT ts AT TIME ZONE 'Europe/Paris' AS ts_local, page, delta, url
      FROM wiki_rc
      ORDER BY ts DESC
      LIMIT %s
    """,[n])

# ===================== UI =====================

tab_flux, tab_now, tab_1h, tab_24h = st.tabs(["Flux direct", "Analyse directe (10 min)", "1 h", "24 h"])

# --------- Flux direct ---------
with tab_flux:
    c1, c2 = st.columns([1.1, 1])
    with c1:
        st.subheader("Derniers articles")
        col_une, col_cont = st.columns(2)
        with col_une:
            st.caption("Flux UNE")
            ln = last_news(20, "une")
            if ln.empty:
                st.caption("— En attente…")
            else:
                for _,r in ln.iterrows():
                    st.markdown(f"**[{r['source']}]** [{r['title']}]({r['url']}) — {r['ts_local']}")
        with col_cont:
            st.caption("Flux continu")
            lc = last_news(20, "continu")
            if lc.empty:
                st.caption("— En attente…")
            else:
                for _,r in lc.iterrows():
                    st.markdown(f"**[{r['source']}]** [{r['title']}]({r['url']}) — {r['ts_local']}")
    with c2:
        st.subheader("Derniers événements Wikipedia")
        lw = wiki_last(25)
        if lw.empty:
            st.caption("— En attente d’événements…")
        else:
            for _,r in lw.iterrows():
                st.markdown(f"**WIKI** [{r['page']}]({r['url']}) — Δ {int(r['delta'])} — {r['ts_local']}")

# --------- Analyse directe (10 min) ---------
with tab_now:
    st.subheader("Top flux continu (10 min)")
    now_df = news_since(10, "continu")
    titles = now_df["title"].astype(str).tolist() if not now_df.empty else []
    trends_now = compute_trends_df(titles)
    if trends_now.empty:
        st.info("En attente de tendances…")
    else:
        top3 = trends_now.head(3).reset_index(drop=True)
        c = st.columns(3)
        for i,(idx,row) in enumerate(top3.iterrows()):
            c[i].metric(label=f"#{i+1}", value=row["phrase"], delta=int(row["score"]))
        st.bar_chart(trends_now.head(12).set_index("phrase")["score"], use_container_width=True)
        img = wc_from_titles(titles)
        if img: st.image(img, caption="WordCloud — 10 min", use_container_width=True)

# --------- 1 h ---------
with tab_1h:
    st.subheader("Tendances flux continu — 60 min")
    df1 = news_since(60, "continu")
    titles1 = df1["title"].astype(str).tolist() if not df1.empty else []
    t1 = compute_trends_df(titles1)
    if t1.empty:
        st.info("En attente de tendances (1 h)…")
    else:
        st.bar_chart(t1.head(20).set_index("phrase")["score"], use_container_width=True)
        img1 = wc_from_titles(titles1)
        if img1: st.image(img1, caption="WordCloud — 1 h", use_container_width=True)

# --------- 24 h ---------
with tab_24h:
    st.subheader("Tendances flux UNE — 24 h")
    dfD = news_since(1440, "une")
    titlesD = dfD["title"].astype(str).tolist() if not dfD.empty else []
    tD = compute_trends_df(titlesD)
    if tD.empty:
        st.info("En attente de tendances (24 h)…")
    else:
        st.bar_chart(tD.head(25).set_index("phrase")["score"], use_container_width=True)
        imgD = wc_from_titles(titlesD)
        if imgD: st.image(imgD, caption="WordCloud — 24 h", use_container_width=True)

st.caption(f"Auto-refresh {REFRESH}s"); time.sleep(REFRESH); st.rerun()
