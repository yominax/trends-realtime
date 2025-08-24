# Wikipedia + HackerNews — Trends Live (Kafka + Postgres + Streamlit)
## Démarrage
```bash
cd wiki-hn-trends-v3
docker compose up -d --build
# UI: http://localhost:8501
```
## Services
- wiki-producer: SSE Wikipedia → Kafka
- hn-producer: API HackerNews → Kafka
- db-writer: Kafka → Postgres
- spike-aggregator: calcule spikes (chaque minute)
- ui: Streamlit
