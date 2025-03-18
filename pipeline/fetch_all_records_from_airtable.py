from airtable import Airtable
import json
from utils.secret_manager import access_secret
import logging
import time

PROJECT_ID = "apollo-432603"
AIRTABLE_SECRET_NAME = "tadesse_airtable_API"
AIRTABLE_BASE_KEY = "appVi25LV7lINvOSC"
AIRTABLE_TABLE_NAME = "Imported table"

def fetch_index_data_from_airtable():
    """
    Fetch calculated sentiment indices from Airtable.
    """
    try:
        # Retrieve the Airtable API key
        airtable_api_key = access_secret(PROJECT_ID, AIRTABLE_SECRET_NAME)
        airtable = Airtable(AIRTABLE_BASE_KEY, AIRTABLE_TABLE_NAME, api_key=airtable_api_key)

        # Fetch all records from Airtable
        records = airtable.get_all()
        logging.info(f"Fetched {len(records)} records from Airtable.")

        # Convert records to a list of dictionaries
        all_data = []
        for record in records:
            fields = record.get("fields", {})
            all_data.append({
                "Headline": fields.get("Headline", ""),
                "Publication": fields.get("Publication", ""),
                "Date": fields.get("Date", ""),
                "Prep_Identify_Duplicate_Syndicate": fields.get("Prep_Identify_Duplicate_Syndicate", 1),
                "AI - Analysis - Results": fields.get("AI - Analysis - Results", {}),
            })

        return all_data
    except Exception as e:
        logging.error(f"Error fetching data from Airtable: {e}")
        return []