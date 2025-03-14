from prometheus_client import start_http_server, Counter, Gauge
import time

# Define metrics
ARTICLES_PROCESSED = Counter("articles_processed_total", "Total number of articles processed")
SENTIMENT_SCORE = Gauge("sentiment_score", "Current sentiment score")
PROCESSING_TIME = Gauge("processing_time_seconds", "Time taken to process the pipeline")

def start_monitoring():
    """
    Start Prometheus metrics server.
    """
    start_http_server(8000)