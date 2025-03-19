import logging
import re
from datetime import datetime, timedelta
from airtable import Airtable
from utils.secret_manager import access_secret
from pipeline.sentiment_analysis import analyze_relevance, analyze_sentiment
from utils.date_utils import process_date

def prep_article_relevance_check(record):
    """
    Task 7: Article Relevance Check
    Uses OpenAI API to analyze articles for sentiment, relevance, and key attributes.
    Stores the structured JSON response in the record.

    Scoring System:
    1 = Analysis successful (JSON response stored).
    0 = Analysis failed (error or missing data).

    Args:
        record (dict): The record containing the article data.

    Returns:
        int: 1 if analysis is successful, 0 otherwise.
    """
    headline = record.get("Headline", "")
    body = record.get("Body", "") or record.get("Content", "")
    if not headline or not body:
        logging.warning("Headline or body is missing in the record.")
        return 0

    try:
        # Step 1: Perform Task 7 (Relevance Filter)
        relevance_score = analyze_relevance(headline, body)
        if relevance_score is None or relevance_score == 0:
            logging.info(f"Article not relevant. Skipping sentiment analysis. Headline: {headline}")
            return 0

        # Step 2: Perform Task 8 (Sentiment Analysis)
        sentiment_data = analyze_sentiment(headline, body)
        if sentiment_data:
            # Store the sentiment and relevance data in the record
            record["Sentiment Data"] = sentiment_data
            record["Overall_Sentiment"] = sentiment_data.get("Overall_Sentiment", "")
            logging.info(f"Sentiment and relevance analysis completed for article: {headline}")
            return 1  # Analysis successful
        else:
            logging.warning(f"Sentiment analysis failed for article: {headline}")
            return 0  # Analysis failed
    except Exception as e:
        logging.error(f"Error analyzing article for sentiment and relevance: {e}")
        return 0  # Analysis failed