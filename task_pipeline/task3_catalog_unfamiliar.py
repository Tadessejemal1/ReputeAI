
import logging
import re
from datetime import datetime, timedelta
from airtable import Airtable
from utils.secret_manager import access_secret
from pipeline.sentiment_analysis import analyze_relevance, analyze_sentiment
from utils.date_utils import process_date
from fuzzywuzzy import process
from utils.fetch_publication import fetch_publication_normalization_dict

def prep_catalog_unfamiliar_publications(record, context):
    """
    Task 3: Catalog Unfamiliar Publications
    Checks if a publication exists in the catalog, and if not, adds it to the context.
    """
    publication = record.get("Publication", "")
    if not publication:
        logging.warning("Publication name is missing in the record.")
        return 0  # Skip if publication is missing

    # Fetch the publication normalization dictionary
    normalization_dict = fetch_publication_normalization_dict()
    if not normalization_dict:
        logging.error("Failed to fetch publication normalization dictionary.")
        return 0

    # Check if the publication exists in the catalog
    if publication.lower() not in normalization_dict:
        # Add the unfamiliar publication to the context
        context.setdefault("catalog_additions", []).append(publication)
        logging.info(f"Added unfamiliar publication to catalog_additions: {publication}")
        return 1  # Publication is unfamiliar
    else:
        logging.info(f"Publication '{publication}' is already in the catalog.")
        return 0  # Publication is familiar

