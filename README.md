# Wikipedia  Trends Live (Kafka + Postgres + Streamlit)
## Démarrage
```bash
cd tends-realtime

docker compose up -d --build
# UI: http://localhost:8501
```
## Services
- wiki-producer: SSE Wikipedia → Kafka
- db-writer: Kafka → Postgres
- spike-aggregator: calcule spikes (chaque minute)
- ui: Streamlit
- news-producer: flux « Une » et flux continu via RSS francophones (+GDELT/Mediastack en option)
