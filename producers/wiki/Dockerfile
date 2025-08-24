FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY wiki_producer.py wiki_producer.py
CMD ["python", "wiki_producer.py"]
