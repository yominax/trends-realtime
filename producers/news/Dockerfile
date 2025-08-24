FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY news_producer.py news_producer.py
CMD ["python", "news_producer.py"]
