import logging
from airtable import Airtable
from utils.secret_manager import access_secret

def fetch_publication_normalization_dict():
    """
    Fetches the publication normalization dictionary from Airtable.
    """
    # Airtable configuration
    PROJECT_ID = "apollo-432603"
    AIRTABLE_BASE_KEY = "appoYTVFYy4tsXKtI"  # Ensure no trailing period
    AIRTABLE_TABLE_NAME = "Press Outlets"
    AIRTABLE_SECRET_NAME = "tadesse_airtable_API"

    # Fetch Airtable API key
    try:
        airtable_api_key = access_secret(PROJECT_ID, AIRTABLE_SECRET_NAME)
        logging.info("Airtable API key fetched successfully.")
    except Exception as e:
        logging.error(f"Failed to fetch Airtable API key: {e}")
        return {}

    # Initialize Airtable
    try:
        airtable = Airtable(AIRTABLE_BASE_KEY, AIRTABLE_TABLE_NAME, api_key=airtable_api_key)
        logging.info("Airtable initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Airtable: {e}")
        return {}

    # Fetch all records from the table
    try:
        records = airtable.get_all()
        logging.info(f"Fetched {len(records)} records from Airtable.")
    except Exception as e:
        logging.error(f"Failed to fetch records from Airtable: {e}")
        return {}

    # Create the normalization dictionary
    PUBLICATION_NORMALIZATION_DICT = {}
    for record in records:
        raw_name = record["fields"].get("Raw Name", "").lower().strip()
        normalized_name = record["fields"].get("Normalized Name", "").strip()
        if raw_name and normalized_name:
            PUBLICATION_NORMALIZATION_DICT[raw_name] = normalized_name

    logging.info(f"Created normalization dictionary with {len(PUBLICATION_NORMALIZATION_DICT)} entries.")
    return PUBLICATION_NORMALIZATION_DICT

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)  # Enable logging
    try:
        normalization_dict = fetch_publication_normalization_dict()
        print(normalization_dict)
    except Exception as e:
        logging.error(f"An error occurred: {e}")