import logging
import re
from datetime import datetime, timedelta
from airtable import Airtable
from utils.secret_manager import access_secret
from pipeline.sentiment_analysis import analyze_relevance, analyze_sentiment
from utils.date_utils import process_date
from fuzzywuzzy import process

from airtable import Airtable

# Publication normalization dictionary
PUBLICATION_NORMALIZATION_DICT = {
    "The New York Times": "The New York Times",
    "TECHNOLOGY RESELLER": "TECHNOLOGY RESELLER",
    "CNN": "CNN",
    "Newbury Today": "Newbury Today",
    "The Wall Street Journal": "The Wall Street Journal",
    "BBC": "BBC",
    "WIRED": "WIRED",
    "NPR": "NPR",
    "Reuters": "Reuters",
    "CBS News": "CBS News",
    "eTeknix": "eTeknix",
}

def prep_normalize_publication_titles(publication_name):
    """
    Task 2: Publication Title Normalization
    Standardizes publication names by:
    1. Stripping domain extensions (e.g., .com).
    2. Removing punctuation.
    3. Converting to lowercase.
    4. Using a reference dictionary for known synonyms.

    Scoring System (0 or 1):
    1 = Publication name successfully normalized.
    0 = No valid normalization match found.

    Args:
        publication_name (str): The raw publication name.

    Returns:
        tuple: (normalized_name, score)
            - normalized_name (str): The normalized publication name.
            - score (int): 1 if normalized, 0 otherwise.
    """
    if not publication_name:
        logging.warning("Publication name is missing.")
        return None, 0

    # Step 1: Remove domain extensions and special characters
    publication_name = re.sub(r"\.com$", "", publication_name)  # Remove .com suffix
    publication_name = re.sub(r"[^\w\s]", "", publication_name)  # Remove punctuation
    publication_name = publication_name.lower().strip()  # Convert to lowercase and trim whitespace

    # Step 2: Use fuzzy matching to find the closest match in the dictionary
    match, score = process.extractOne(publication_name, PUBLICATION_NORMALIZATION_DICT.keys())

    # Step 3: Set a threshold for acceptable matches (e.g., 80 out of 100)
    if score >= 80:
        normalized_name = PUBLICATION_NORMALIZATION_DICT[match]
        logging.info(f"Normalized publication name: {publication_name} -> {normalized_name}")
        return normalized_name, 1  # Successfully normalized
    else:
        logging.warning(f"No match found for publication: {publication_name}")
        return publication_name, 0  # No valid normalization match found