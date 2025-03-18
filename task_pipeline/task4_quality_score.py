import logging
from airtable import Airtable
from utils.secret_manager import access_secret

def fetch_publication_tier(publication):
    """
    Fetches the publication tier from the Publication Dictionary Airtable.
    Returns a JSON object with success, tier, and error details.
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
        error_message = f"Failed to fetch Airtable API key: {e}"
        logging.error(error_message)
        return {
            "success": False,
            "error_code": "E4_API_KEY",
            "error_message": error_message,
        }

    # Initialize Airtable
    try:
        airtable = Airtable(AIRTABLE_BASE_KEY, AIRTABLE_TABLE_NAME, api_key=airtable_api_key)
        logging.info("Airtable initialized successfully.")
    except Exception as e:
        error_message = f"Failed to initialize Airtable: {e}"
        logging.error(error_message)
        return {
            "success": False,
            "error_code": "E4_INIT",
            "error_message": error_message,
        }

    # Fetch records matching the publication name
    try:
        records = airtable.get_all(formula=f"SEARCH('{publication}', {{primary_name}})")
        logging.info(f"Fetched {len(records)} records for publication: {publication}")
    except Exception as e:
        error_message = f"Failed to fetch records from Airtable: {e}"
        logging.error(error_message)
        return {
            "success": False,
            "error_code": "E4_FETCH",
            "error_message": error_message,
        }

    # Extract the publication tier from the first matching record
    if records:
        publication_tier = records[0]["fields"].get("publication_tier")
        if publication_tier is not None:
            logging.info(f"Found publication tier for '{publication}': {publication_tier}")
            return {
                "success": True,
                "tier": publication_tier,
            }
        else:
            error_message = f"No publication tier found for '{publication}'."
            logging.warning(error_message)
            return {
                "success": False,
                "error_code": "E4_NO_TIER",
                "error_message": error_message,
            }
    else:
        error_message = f"No matching records found for publication: {publication}"
        logging.warning(error_message)
        return {
            "success": False,
            "error_code": "E4_NO_RECORD",
            "error_message": error_message,
        }

def prep_assign_quality_scores(record, context):
    """
    Task 4: Assign Publication Quality Score
    Assigns a publication tier based on predefined rankings.
    Returns a score of 1 if the publication tier is successfully found, otherwise 0.
    """
    publication = record.get("Publication", "")
    if not publication:
        error_message = "Publication name is missing in the record."
        logging.warning(error_message)
        context.setdefault("errors", []).append({
            "task": "Task 4: Assign Publication Quality Score",
            "error_code": "E4_NO_PUBLICATION",
            "error_message": error_message,
        })
        return 0

    # Fetch the publication tier from Airtable
    tier_result = fetch_publication_tier(publication)
    if tier_result["success"]:
        # Assign the tier to the record
        record["Publication_Tier"] = tier_result["tier"]
        logging.info(f"Assigned publication tier {tier_result['tier']} to '{publication}'.")
        return 1  # Successfully found a publication tier
    else:
        # Log the error in the context
        context.setdefault("errors", []).append({
            "task": "Task 4: Assign Publication Quality Score",
            "error_code": tier_result["error_code"],
            "error_message": tier_result["error_message"],
        })
        logging.warning(f"Failed to assign publication tier for '{publication}': {tier_result['error_message']}")
        return 0  # Tier not found or an error occurred