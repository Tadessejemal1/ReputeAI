import logging
from airtable import Airtable
from utils.secret_manager import access_secret

def fetch_publication_normalization_dict():
    """
    Fetches the publication normalization dictionary from Airtable.
    Uses `primary_name` as the normalized name and `original_name` or `variations` as raw names.
    Also fetches the `publication_tier` for each publication.
    """
    # Airtable configuration
    PROJECT_ID = "apollo-432603"
    AIRTABLE_BASE_KEY = "appoYTVFYy4tsXKtI"  # Replace with your actual base ID
    AIRTABLE_TABLE_NAME = "Press Outlets"  # Replace with your actual table name
    AIRTABLE_SECRET_NAME = "tadesse_airtable_API"  # Replace with your actual secret name

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
        fields = record.get("fields", {})
        primary_name = fields.get("primary_name", "").strip()
        original_name = fields.get("original_name", "").strip()
        variations = fields.get("variations", "").strip()
        publication_tier = fields.get("publication_tier")  # Fetch publication tier

        # Determine the normalized name
        if primary_name:
            normalized_name = primary_name
        elif original_name:
            normalized_name = original_name
            logging.warning(f"Using 'original_name' as normalized name for record: {record.get('id')}")
        elif variations:
            normalized_name = variations.split(",")[0].strip()  # Use the first variation
            logging.warning(f"Using first variation as normalized name for record: {record.get('id')}")
        else:
            continue  # Skip this record

        # Add `original_name` as a raw name if it exists
        if original_name:
            PUBLICATION_NORMALIZATION_DICT[original_name.lower()] = {
                "normalized_name": normalized_name,
                "publication_tier": publication_tier,
            }

        # Add variations as raw names if they exist
        if variations:
            for variation in variations.split(","):
                variation = variation.strip().lower()
                if variation:
                    PUBLICATION_NORMALIZATION_DICT[variation] = {
                        "normalized_name": normalized_name,
                        "publication_tier": publication_tier,
                    }

    logging.info(f"Created normalization dictionary with {len(PUBLICATION_NORMALIZATION_DICT)} entries.")
    return PUBLICATION_NORMALIZATION_DICT