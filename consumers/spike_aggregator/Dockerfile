FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY spike_aggregator.py spike_aggregator.py
CMD ["python", "spike_aggregator.py"]
