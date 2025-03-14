import logging
import re
from datetime import datetime, timedelta
from airtable import Airtable
from utils.secret_manager import access_secret
from pipeline.sentiment_analysis import analyze_relevance, analyze_sentiment
from utils.date_utils import process_date
from fuzzywuzzy import process

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
MIN_BODY_LENGTH = 50
PROJECT_ID = "apollo-432603"
AIRTABLE_SECRET_NAME = "tadesse_airtable_API"
AIRTABLE_BASE_KEY = "appVi25LV7lINvOSC"
AIRTABLE_TABLE_NAME = "Imported table"

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

def pre_check_null_values(record):
    """
    Task 1: Pre-Check Null Values
    Validates records for required fields and logs missing fields.
    Accepts both field naming conventions:
    - Search_link or Search
    - Link_to_story or Story_Link
    """
    required_fields = [
        ("Search_link", "Search"),
        "Date",
        "Publication",
        "Author",
        "Headline",
        "Body",
        ("Link_to_story", "Story_Link"),
    ]
    
    missing_fields = []
    for field in required_fields:
        if isinstance(field, tuple):
            # Check for alternative field names
            if not any(record.get(f) for f in field):
                missing_fields.append(f"({', '.join(field)})")
        else:
            # Check for single field name
            if not record.get(field):
                missing_fields.append(field)
    
    if missing_fields:
        logging.warning(f"Missing required fields: {missing_fields}")
        return 0
    else:
        return 1

def auto_delete_short_body_text(record, airtable):
    """
    Task 1: Auto Delete Short Body Text
    Flags or deletes articles whose body text is too short to be meaningful.
    If the body text is too short, the record is deleted from the Airtable table.

    Scoring System (0 or 1):
    1 = Article body meets minimum length requirement.
    0 = Body text is below threshold and excluded (record deleted from Airtable).

    Args:
        record (dict): The record containing the body text.
        airtable (Airtable): The Airtable connection object.

    Returns:
        int: 1 if the body text meets the minimum length requirement, 0 otherwise.
    """
    body = record.get("Body", "") or record.get("Content", "")  # Handle both "Body" and "Content" fields
    if not body:
        logging.warning("Body text is missing.")
        return 0

    # Count the number of words in the body
    word_count = len(body.split())
    if word_count < MIN_BODY_LENGTH:
        logging.warning(f"Body text is too short ({word_count} words). Deleting record from Airtable.")

        # Delete the record from Airtable
        record_id = record.get("requestId")  # Airtable record ID
        if record_id:
            try:
                airtable.delete(record_id)
                logging.info(f"Deleted record with ID {record_id} from Airtable.")
            except Exception as e:
                logging.error(f"Error deleting record with ID {record_id} from Airtable: {e}")
        else:
            logging.warning("Record ID is missing. Cannot delete from Airtable.")

        return 0  # Body text is too short
    else:
        return 1  # Body text meets the minimum length requirement
    
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


def prep_catalog_unfamiliar_publications(record, context):
    """
    Task 3: Catalog Unfamiliar Publications
    Checks if the normalized publication name exists in the Publication Dictionary.
    If unfamiliar, adds it to context['catalog_additions'] for potential creation in Airtable.

    Scoring System (0 or 1):
    1 = Publication was new and added to the catalog.
    0 = Publication was already recognized; no addition needed.

    Args:
        record (dict): The record containing the publication name.
        context (dict): The context dictionary for storing catalog additions.

    Returns:
        int: 1 if publication was new, 0 otherwise.
    """
    publication = record.get("Publication", "")
    if not publication:
        logging.warning("Publication name is missing in the record.")
        return 0

    # Check if the publication exists in the catalog
    if publication not in context.get("catalog", []):
        # Add the publication to catalog_additions for bulk creation in Airtable
        context.setdefault("catalog_additions", []).append(publication)
        logging.info(f"New publication added to catalog: {publication}")
        return 1  # Publication was new and added to the catalog
    else:
        logging.info(f"Publication already recognized: {publication}")
        return 0  # Publication was already recognized; no addition needed

def prep_assign_quality_scores(record):
    """
    Task 4: Assign Publication Quality Score
    Assigns a publication tier based on predefined rankings.
    """
    publication = record.get("Publication", "")
    # Example logic to fetch tier from an external database
    tier = fetch_publication_tier(publication)
    if tier:
        record["Publication_Tier"] = tier
        return 1
    else:
        return 0

def fetch_publication_tier(publication):
    """
    Fetches the publication tier from an external database.
    """
    # Placeholder logic - replace with actual database query
    publication_tiers = {
        "The New York Times": 1,
        "The Wall Street Journal": 1,
        "BBC": 2,
        # Add more mappings as needed
    }
    return publication_tiers.get(publication, None)

def prep_identify_duplicate_syndicate(record, context):
    """
    Task 5: Identify Duplicate/Syndicate Content
    Detects duplicate or syndicated content based on headlines within a ±7-day window.
    """
    headline = record.get("Headline", "")
    date_str = record.get("Date", "")
    if not headline or not date_str:
        return 0

    # Process the date string to extract only the date part
    date_only = process_date(date_str)
    if not date_only:
        return 0  # Skip if date processing fails

    # Convert the processed date to a datetime object
    try:
        record_date = datetime.strptime(date_only, "%Y-%m-%d")
    except ValueError:
        logging.error(f"Invalid processed date format: {date_only}")
        return 0

    # Check for duplicates within ±7 days
    for existing_record in context.get("records", []):
        existing_headline = existing_record.get("Headline", "")
        existing_date_str = existing_record.get("Date", "")
        if existing_headline == headline and existing_date_str:
            # Process the existing record's date
            existing_date_only = process_date(existing_date_str)
            if not existing_date_only:
                continue  # Skip if date processing fails

            try:
                existing_record_date = datetime.strptime(existing_date_only, "%Y-%m-%d")
                if abs((record_date - existing_record_date).days) <= 7:
                    context.setdefault("duplicate_pairs", []).append((record, existing_record))
                    return 0
            except ValueError:
                logging.error(f"Invalid processed date format in existing record: {existing_date_only}")
    return 1

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
    body = record.get("Body", "")
    if not headline or not body:
        logging.warning("Headline or body is missing in the record.")
        return 0

    try:
        # Analyze the article for sentiment and relevance
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

